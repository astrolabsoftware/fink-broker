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

import argparse
import os
import pandas as pd

from fink_broker.parser import getargs
from fink_broker.sparkUtils import init_sparksession

from fink_broker.hbaseUtils import push_to_hbase

from fink_broker.loggingUtils import get_fink_logger, inspect_application


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

    # Table 1: orbital parameters

    # connect to fink_fat_output
    pdf_orb = pd.read_parquet(
        os.path.join(args.fink_fat_output, 'orbital.parquet')
    )

    # renaming
    for col_ in pdf_orb.columns:
        if '. ' in col_:
            pdf_orb = pdf_orb.rename({col_: col_.replace('. ', '_')}, axis='columns')
        if ' ' in col_:
            pdf_orb = pdf_orb.rename({col_: col_.replace(' ', '_')}, axis='columns')

    df_orb = spark.createDataFrame(pdf_orb)

    cf = {i: 'd' for i in df_orb.columns}

    index_row_key_name = 'cand_trajectory_id'
    index_name = '.orb_cand'

    # add the rowkey
    df_cand = df_cand.withColumn(
        index_row_key_name,
        concat_ws('_', *[F.lit('cand'), 'trajectory_id'])
    )

    push_to_hbase(
        df=df_orb,
        table_name=args.science_db_name + index_name,
        rowkeyname=index_row_key_name,
        cf=cf
    )

    # Table 2: candidates

    # connect to fink_fat_output
    pdf_cand = pd.read_parquet(
        os.path.join(args.fink_fat_output, 'trajectory_orb.parquet')
    )

    # drop unused columns
    pdf_cand = pdf_cand.drop(['ssnamenr', 'not_updated'], axis='columns')

    df_cand = spark.createDataFrame(pdf_cand)

    cf = {i: 'd' for i in df_cand.columns}

    index_row_key_name = 'jd_trajectory_id'
    index_name = '.sso_cand'

    # add the rowkey
    df_cand = df_cand.withColumn(
        index_row_key_name,
        concat_ws('_', *['jd', 'trajectory_id'])
    )

    push_to_hbase(
        df=df_cand,
        table_name=args.science_db_name + index_name,
        rowkeyname=index_row_key_name,
        cf=cf
    )


if __name__ == "__main__":
    main()
