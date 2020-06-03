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
"""Retrieve one ZTF day, and merge small files into larger ones.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import argparse
import time
import json
import subprocess

from fink_broker.parser import getargs
from fink_broker.sparkUtils import init_sparksession
from fink_broker.partitioning import jd_to_datetime, numPart
from fink_broker.loggingUtils import get_fink_logger, inspect_application

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(name="mergeAndClean_{}".format(args.night))

    # Logger to print useful debug statements
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    year = args.night[:4]
    month = args.night[4:6]
    day = args.night[6:8]

    print('Processing {}/{}/{}'.format(year, month, day))

    # Uncomment to move old data
    # input_raw = 'ztf_{}{}/alerts_store/topic=ztf_{}{}{}_programid1/year={}/month={}/day={}'.format(
    #     year, month, year, month, day, year, month, day)
    # input_science = 'ztf_{}{}/alerts_store_tmp/year={}/month={}/day={}'.format(
    #     year, month, year, month, day)

    input_raw = '{}/year={}/month={}/day={}'.format(
        args.rawdatapath, year, month, day)
    input_science = '{}/year={}/month={}/day={}'.format(
        args.scitmpdatapath, year, month, day)

    # they do not need to exist
    output_raw = 'ztf_{}/raw'.format(year)
    output_science = 'ztf_{}/science'.format(year)

    print('Raw data processing....')
    df_raw = spark.read.format('parquet').load(input_raw)
    print('Num partitions before: ', df_raw.rdd.getNumPartitions())
    print('Num partitions after : ', numPart(df_raw))

    df_raw.withColumn('ts_jd', jd_to_datetime(df_raw['candidate.jd']))\
        .withColumn("month", F.date_format("ts_jd", "MM"))\
        .withColumn("day", F.date_format("ts_jd", "dd"))\
        .coalesce(numPart(df_raw))\
        .write\
        .mode("append") \
        .partitionBy("month", "day")\
        .parquet(output_raw)

    print('Science data processing....')

    df_science = spark.read.format('parquet').load(input_science)
    print('Num partitions before: ', df_science.rdd.getNumPartitions())
    print('Num partitions after : ', numPart(df_science))

    df_science.withColumn('ts_jd', jd_to_datetime(df_science['candidate.jd']))\
        .withColumn("month", F.date_format("ts_jd", "MM"))\
        .withColumn("day", F.date_format("ts_jd", "dd"))\
        .coalesce(numPart(df_science))\
        .write\
        .mode("append") \
        .partitionBy("month", "day")\
        .parquet(output_science)

    # Remove temporary alert folder - beware you'll never get it back!
    if args.fs == 'hdfs':
        subprocess.run(["hdfs", "dfs", '-rm', '-rf', args.datapath])
    elif args.fs == 'local':
        subprocess.run(['rm', '-rf', args.datapath])
    else:
        print('Filesystem not understood. FS_KIND must be hdfs or local.')


if __name__ == "__main__":
    main()
