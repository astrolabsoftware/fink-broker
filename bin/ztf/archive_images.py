#!/usr/bin/env python
# Copyright 2023-2024 AstroLab Software
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
"""Push image data metadata to an HBase table"""

import argparse

import pyspark.sql.functions as F

from fink_broker.ztf.parser import getargs
from fink_broker.common.spark_utils import init_sparksession, load_parquet_files
from fink_broker.ztf.hbase_utils import assign_column_family_names
from fink_broker.ztf.hbase_utils import push_to_hbase, add_row_key

from fink_broker.common.logging_utils import get_fink_logger, inspect_application


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(
        name="image_archival_{}".format(args.night), shuffle_partitions=2
    )

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    # Connect to the aggregated science database
    folder = "{}/science/year={}/month={}/day={}".format(
        args.agg_data_prefix, args.night[:4], args.night[4:6], args.night[6:8]
    )

    df = load_parquet_files(folder)

    df = df.withColumn("hdfs_path", F.input_file_name())

    cols = [
        "objectId",
        "candidate.jd",
        "candid",
        "hdfs_path",
    ]

    df = df.select(cols)

    # Push to HBase
    row_key_name = "objectId_jd"

    cf = assign_column_family_names(
        df,
        cols_i=["objectId", "candid", "jd"],
        cols_d=["hdfs_path"],
    )

    df = add_row_key(df, row_key_name=row_key_name, cols=row_key_name.split("_"))

    push_to_hbase(
        df=df,
        table_name=args.science_db_name + ".cutouts",
        rowkeyname=row_key_name,
        cf=cf,
        catfolder=args.science_db_catalogs,
    )


if __name__ == "__main__":
    main()
