#!/usr/bin/env python
# Copyright 2020 AstroLab Software
# Author: Julien Peloton
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Push science data to the science portal (HBase table)

1. Use the Alert data that is stored in the Science TMP database (Parquet)
2. Extract relevant information from alerts
3. Construct HBase catalog
4. Push data (single shot)
"""
from pyspark.sql.functions import lit, concat_ws, col

import argparse
import time
import json

from fink_broker import __version__ as fbvsn
from fink_broker.parser import getargs
from fink_broker.sparkUtils import init_sparksession, load_parquet_files

from fink_broker.hbaseUtils import construct_hbase_catalog_from_flatten_schema
from fink_broker.hbaseUtils import load_science_portal_column_names
from fink_broker.hbaseUtils import assign_column_family_names
from fink_broker.hbaseUtils import attach_rowkey
from fink_broker.hbaseUtils import construct_schema_row
from fink_broker.science import ang2pix, extract_fink_classification

from fink_broker.loggingUtils import get_fink_logger, inspect_application

from fink_science import __version__ as fsvsn

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(
        name="index_archival_{}_{}".format(args.index_table, args.night),
        shuffle_partitions=2
    )

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    # Push data monthly
    path = 'ztf_alerts/science_reprocessed/year={}/month={}/day={}'.format(
        args.night[:4],
        args.night[4:6],
        args.night[6:8]
    )
    df = load_parquet_files(path)

    # Drop partitioning columns
    df = df.drop('year').drop('month').drop('day')

    # Load column names to use in the science portal
    cols_i, cols_d, cols_b = load_science_portal_column_names()

    # Assign each column to a specific column family
    cf = assign_column_family_names(df, cols_i, cols_d, cols_b)

    # Restrict the input DataFrame to the subset of wanted columns.
    df = df.select(cols_i + cols_d + cols_b)

    # Create and attach the rowkey
    df, row_key_name = attach_rowkey(df)

    # construct the index view
    index_row_key_name = args.index_table
    columns = index_row_key_name.split('_')
    names = [col(i) for i in columns]
    index_name = '.' + columns[0] # = 'jd'

    if columns[0] == 'pixel':
        df_index = df.withColumn('pixel', ang2pix(df['ra'], df['dec'], lit(131072))).select(
            [
                concat_ws('_', *names).alias(index_row_key_name),
                'objectId',
                'ra', 'dec', 'jd', 'cdsxmatch', 'ndethist',
                'roid', 'mulens_class_1',
                'mulens_class_2', 'snn_snia_vs_nonia',
                'snn_sn_vs_all', 'rfscore',
            ]
        )
    elif columns[0] == 'class':
        df_index = df.withColumn('class', extract_fink_classification(df['cdsxmatch'], df['roid'], df['mulens_class_1'], df['mulens_class_2'], df['snn_snia_vs_nonia'], df['snn_sn_vs_all'])).select(
            [
                concat_ws('_', *names).alias(index_row_key_name),
                'objectId',
                'ra', 'dec', 'jd', 'cdsxmatch', 'ndethist',
                'roid', 'mulens_class_1',
                'mulens_class_2', 'snn_snia_vs_nonia',
                'snn_sn_vs_all', 'rfscore',
            ]
        )
    else:
        df_index = df.select(
            [
                concat_ws('_', *names).alias(index_row_key_name),
                'objectId',
                'ra', 'dec', 'jd', 'cdsxmatch', 'ndethist',
                'roid', 'mulens_class_1',
                'mulens_class_2', 'snn_snia_vs_nonia',
                'snn_sn_vs_all', 'rfscore'
            ]
        )

    # construct the time catalog
    hbcatalog_index = construct_hbase_catalog_from_flatten_schema(
        df_index.schema, args.science_db_name + index_name, rowkeyname=index_row_key_name, cf=cf)

    # Push index table
    df_index.write\
        .options(catalog=hbcatalog_index, newtable=50)\
        .format("org.apache.spark.sql.execution.datasources.hbase")\
        .save()

    # Construct the schema row - inplace replacement
    schema_row_key_name = 'schema_version'
    df_index = df_index.withColumnRenamed(index_row_key_name, schema_row_key_name)

    df_index_schema = construct_schema_row(
        df_index,
        rowkeyname=schema_row_key_name,
        version='schema_{}_{}'.format(fbvsn, fsvsn))

    # construct the hbase catalog for the schema
    hbcatalog_index_schema = construct_hbase_catalog_from_flatten_schema(
        df_index_schema.schema,
        args.science_db_name + index_name,
        rowkeyname=schema_row_key_name,
        cf=cf)

    # Push the data using the shc connector
    df_index_schema.write\
        .options(catalog=hbcatalog_index_schema, newtable=50)\
        .format("org.apache.spark.sql.execution.datasources.hbase")\
        .save()


if __name__ == "__main__":
    main()
