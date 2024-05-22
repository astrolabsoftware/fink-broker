#!/usr/bin/env python
# Copyright 2020-2024 AstroLab Software
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
"""Retrieve one ZTF day, and merge small files into larger ones."""
from pyspark.sql import functions as F

import argparse

from fink_broker.parser import getargs
from fink_broker.sparkUtils import init_sparksession
from fink_broker.partitioning import convert_to_datetime, compute_num_part
from fink_broker.loggingUtils import get_fink_logger, inspect_application
from fink_broker.tracklet_identification import add_tracklet_information

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

    input_raw = '{}/raw/year={}/month={}/day={}'.format(
        args.online_data_prefix, year, month, day)
    input_science = '{}/science/year={}/month={}/day={}'.format(
        args.online_data_prefix, year, month, day)

    # basepath
    output_raw = '{}/raw'.format(args.agg_data_prefix)
    output_science = '{}/science'.format(args.agg_data_prefix)

    print('Raw data processing....')
    df_raw = spark.read.format('parquet').load(input_raw)
    print('Num partitions before: ', df_raw.rdd.getNumPartitions())
    print('Num partitions after : ', compute_num_part(df_raw))

    df_raw.withColumn('timestamp', convert_to_datetime(df_raw['candidate.jd']))\
        .withColumn("year", F.date_format("timestamp", "yyyy"))\
        .withColumn("month", F.date_format("timestamp", "MM"))\
        .withColumn("day", F.date_format("timestamp", "dd"))\
        .coalesce(compute_num_part(df_raw))\
        .write\
        .mode("append") \
        .partitionBy("year", "month", "day")\
        .parquet(output_raw)

    print('Science data processing....')

    df_science = spark.read.format('parquet').load(input_science)
    npart_after = int(compute_num_part(df_science))
    print('Num partitions before: ', df_science.rdd.getNumPartitions())
    print('Num partitions after : ', npart_after)

    # Add tracklet information before merging
    df_trck = add_tracklet_information(df_science)

    # join back information to the initial dataframe
    df_science = df_science\
        .join(
            F.broadcast(df_trck.select(['candid', 'tracklet'])),
            on='candid',
            how='outer'
        )

    df_science.withColumn('timestamp', convert_to_datetime(df_science['candidate.jd']))\
        .withColumn("year", F.date_format("timestamp", "yyyy"))\
        .withColumn("month", F.date_format("timestamp", "MM"))\
        .withColumn("day", F.date_format("timestamp", "dd"))\
        .coalesce(npart_after)\
        .write\
        .mode("append") \
        .partitionBy("year", "month", "day")\
        .parquet(output_science)


if __name__ == "__main__":
    main()
