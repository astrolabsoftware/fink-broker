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
    for col_ in pdf.columns:
        if '. ' in col_:
            pdf = pdf.rename({col_: col_.replace('. ', '_')}, axis='columns')
        if ' ' in col_:
            pdf = pdf.rename({col_: col_.replace(' ', '_')}, axis='columns')

    # Question: do we need ssnamenr column?
    df_orb = spark.createDataFrame(pdf_orb)

    cf = {i: 'd' for i in df_orb.columns}

    index_row_key_name = 'trajectory_id'
    index_name = '.orb_cand'

    push_to_hbase(
        df=df_orb,
        table_name=args.science_db_name + index_name,
        rowkeyname=index_row_key_name,
        cf=cf
    )


if __name__ == "__main__":
    main()
