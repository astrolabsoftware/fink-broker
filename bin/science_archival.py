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
from pyspark.sql.functions import lit

import argparse
import time
import json

from fink_broker.parser import getargs
from fink_broker.sparkUtils import init_sparksession, load_parquet_files

from fink_broker.hbaseUtils import construct_hbase_catalog_from_flatten_schema
from fink_broker.hbaseUtils import load_science_portal_column_names
from fink_broker.hbaseUtils import assign_column_family_names
from fink_broker.hbaseUtils import attach_rowkey
from fink_broker.hbaseUtils import construct_schema_row

from fink_broker.loggingUtils import get_fink_logger, inspect_application

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(name="science_archival", shuffle_partitions=2)

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    # Connect to the TMP science database
    df = load_parquet_files(args.night_to_archive)

    # Drop partitioning columns
    df = df.drop('year').drop('month').drop('day').drop('hour')

    # Switch publisher
    df = df.withColumn('publisher-tmp', lit('Fink')) \
        .drop('publisher') \
        .withColumnRenamed('publisher-tmp', 'publisher')

    # Load column names to use in the science portal
    cols_i, cols_d, cols_b = load_science_portal_column_names()

    # Assign each column to a specific column family
    cf = assign_column_family_names(df, cols_i, cols_d, cols_b)

    # Restrict the input DataFrame to the subset of wanted columns.
    df = df.select(cols_i + cols_d + cols_b)

    # Create and attach the rowkey
    df, row_key_name = attach_rowkey(df)

    # construct the hbase catalog
    hbcatalog = construct_hbase_catalog_from_flatten_schema(
        df.schema, args.science_db_name, rowkeyname=row_key_name, cf=cf)

    # Save the catalog on disk (local)
    with open(args.science_db_catalog, 'w') as json_file:
        json.dump(hbcatalog, json_file)

    if args.save_science_db_catalog_only:
        # Print for visual inspection
        print(hbcatalog)
    else:
        # Push the data using the shc connector
        df.write\
            .options(catalog=hbcatalog, newtable=5)\
            .format("org.apache.spark.sql.execution.datasources.hbase")\
            .save()

        # Construct the schema row - inplace replacement
        schema_row_key_name = 'schema_version'
        df = df.withColumnRenamed(row_key_name, schema_row_key_name)

        df_schema = construct_schema_row(
            df,
            rowkeyname=schema_row_key_name,
            version='schema_v0')

        # construct the hbase catalog for the schema
        hbcatalog_schema = construct_hbase_catalog_from_flatten_schema(
            df_schema.schema,
            args.science_db_name,
            rowkeyname=schema_row_key_name,
            cf=cf)

        # Save the catalog on disk (local)
        catname = args.science_db_catalog.replace('.json', '_schema_row.json')
        with open(catname, 'w') as json_file:
            json.dump(hbcatalog_schema, json_file)

        # Push the data using the shc connector
        df_schema.write\
            .options(catalog=hbcatalog_schema, newtable=5)\
            .format("org.apache.spark.sql.execution.datasources.hbase")\
            .save()


if __name__ == "__main__":
    main()
