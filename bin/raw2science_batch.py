#!/usr/bin/env python
# Copyright 2019-2022 AstroLab Software
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
"""Batch version of raw2science.py to re-process data of one night
"""
from pyspark.sql import functions as F

import argparse

from fink_broker import __version__ as fbvsn
from fink_broker.parser import getargs
from fink_broker.sparkUtils import init_sparksession
from fink_broker.loggingUtils import get_fink_logger, inspect_application
from fink_broker.partitioning import convert_to_datetime
from fink_broker.tracklet_identification import add_tracklet_information
from fink_broker.science import apply_science_modules

from fink_science import __version__ as fsvsn

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(
        name="raw2science_{}".format(args.night),
        shuffle_partitions=None)

    # Logger to print useful debug statements
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    year = args.night[:4]
    month = args.night[4:6]
    day = args.night[6:8]

    print('Processing {}/{}/{}'.format(year, month, day))

    # data path
    input_raw = args.agg_data_prefix + '/raw/year={}/month={}/day={}'.format(
        year, month, day)

    # basepath
    output_science = args.agg_data_prefix + '/science'

    df = spark.read.format('parquet').load(input_raw)
    npart = df.rdd.getNumPartitions()

    # Apply level one filters
    logger.info("Applying quality cuts")
    df = df.filter(df['candidate.nbad'] == 0).filter(df['candidate.rb'] >= 0.55)

    # Apply science modules
    df = apply_science_modules(df, logger)

    # Add tracklet information
    df_trck = spark.read.format('parquet').load(input_raw)
    df_trck = df_trck\
        .filter(df_trck['candidate.nbad'] == 0)\
        .filter(df_trck['candidate.rb'] >= 0.55)
    df_trck = add_tracklet_information(df_trck)

    # join back information to the initial dataframe
    df = df\
        .join(
            F.broadcast(df_trck.select(['candid', 'tracklet'])),
            on='candid',
            how='outer'
        )

    # Add librarys versions
    df = df.withColumn('fink_broker_version', F.lit(fbvsn))\
        .withColumn('fink_science_version', F.lit(fsvsn))

    # Switch publisher
    df = df.withColumn('publisher', F.lit('Fink'))

    df.coalesce(npart)\
        .withColumn("timestamp", convert_to_datetime(df['candidate.jd']))\
        .withColumn("year", F.date_format("timestamp", "yyyy"))\
        .withColumn("month", F.date_format("timestamp", "MM"))\
        .withColumn("day", F.date_format("timestamp", "dd"))\
        .write\
        .mode("append") \
        .partitionBy("year", "month", "day")\
        .parquet(output_science)


if __name__ == "__main__":
    main()
