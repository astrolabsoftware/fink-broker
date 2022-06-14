#!/usr/bin/env python
# Copyright 2022 AstroLab Software
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
from pyspark.sql.functions import arrays_zip, explode
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StringType

import argparse
import os
import numpy as np
import pandas as pd

from fink_broker import __version__ as fbvsn
from fink_broker.parser import getargs
from fink_broker.sparkUtils import init_sparksession, load_parquet_files

from fink_broker.hbaseUtils import construct_hbase_catalog_from_flatten_schema
from fink_broker.hbaseUtils import load_science_portal_column_names
from fink_broker.hbaseUtils import assign_column_family_names
from fink_broker.hbaseUtils import attach_rowkey
from fink_broker.hbaseUtils import construct_schema_row
from fink_broker.science import ang2pix

from fink_filters.classification import extract_fink_classification

from fink_tns.utils import download_catalog

from astropy.coordinates import SkyCoord
from astropy import units as u

from fink_broker.loggingUtils import get_fink_logger, inspect_application

from fink_science import __version__ as fsvsn


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(
        name="index_sso_cand_archival_{}".format(args.night),
        shuffle_partitions=2
    )

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    # connect to fink_fat_output
    pdf_orb = pd.read_parquet(
        os.path.join(args.fink_fat_output, 'trajectory_orb.parquet')
    )

    # Question: do we need ssnamenr column?
    df_orb = spark.createDataFrame(pdf_orb)

    cf = {i: 'd' for i in df_orb.columns}

    index_row_key_name = 'trajectory_id'
    index_name = '.orb_cand'

    # construct the time catalog
    hbcatalog_index = construct_hbase_catalog_from_flatten_schema(
        df_orb.schema,
        args.science_db_name + index_name,
        rowkeyname=index_row_key_name,
        cf=cf
    )

    # Push index table
    df_orb.write\
        .options(catalog=hbcatalog_index, newtable=50)\
        .format("org.apache.hadoop.hbase.spark")\
        .option("hbase.spark.use.hbasecontext", False)\
        .save()

    # Construct the schema row - inplace replacement
    schema_row_key_name = 'schema_version'
    df_orb = df_orb.withColumnRenamed(
        index_row_key_name,
        schema_row_key_name
    )

    df_index_schema = construct_schema_row(
        df_orb,
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
        .format("org.apache.hadoop.hbase.spark")\
        .option("hbase.spark.use.hbasecontext", False)\
        .save()


if __name__ == "__main__":
    main()
