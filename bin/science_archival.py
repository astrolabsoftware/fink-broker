#!/usr/bin/env python
# Copyright 2020-2022 AstroLab Software
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
import argparse

from fink_broker.parser import getargs
from fink_broker.sparkUtils import init_sparksession, load_parquet_files

from fink_broker.hbaseUtils import push_to_hbase
from fink_broker.hbaseUtils import load_science_portal_column_names
from fink_broker.hbaseUtils import assign_column_family_names
from fink_broker.hbaseUtils import attach_rowkey

from fink_broker.loggingUtils import get_fink_logger, inspect_application

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(
        name="science_archival_{}".format(args.night),
        shuffle_partitions=2
    )

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    # Connect to the aggregated science database
    path = '{}/science/year={}/month={}/day={}'.format(
        args.agg_data_prefix,
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

    push_to_hbase(
        df=df,
        table_name=args.science_db_name,
        rowkeyname=row_key_name,
        cf=cf,
        catfolder=args.science_db_catalogs
    )


if __name__ == "__main__":
    main()
