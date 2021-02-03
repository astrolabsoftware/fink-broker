#!/usr/bin/env python
# Copyright 2019-2021 AstroLab Software
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
from fink_broker.filters import apply_user_defined_filter
from fink_broker.loggingUtils import get_fink_logger, inspect_application

from fink_broker.science import apply_science_modules

from fink_science import __version__ as fsvsn

qualitycuts = 'fink_broker.filters.qualitycuts'

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(
        name="raw2science_{}".format(args.night),
        shuffle_partitions=2)

    # Logger to print useful debug statements
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    year = args.night[:4]
    month = args.night[4:6]
    day = args.night[6:8]

    print('Processing {}/{}/{}'.format(year, month, day))

    input_raw = 'ztf_alerts/raw/year={}/month={}/day={}'.format(
        year, month, day)

    # basepath
    output_science = 'ztf_alerts/science_reprocessed'

    df = spark.read.format('parquet').load(input_raw)

    # Apply level one filters
    logger.info(qualitycuts)
    df = apply_user_defined_filter(df, qualitycuts)

    # Apply science modules
    df = apply_science_modules(df, logger)

    # Add librarys versions
    df = df.withColumn('fink_broker_version', F.lit(fbvsn))\
        .withColumn('fink_science_version', F.lit(fsvsn))

    # Switch publisher
    df = df.withColumn('publisher', F.lit('Fink'))

    # re-create partitioning columns.
    # Partitioned data doesn't preserve type information (cast as int...)
    df\
        .withColumn("year", F.date_format("timestamp", "yyyy"))\
        .withColumn("month", F.date_format("timestamp", "MM"))\
        .withColumn("day", F.date_format("timestamp", "dd"))\
        .write\
        .mode("append") \
        .partitionBy("year", "month", "day")\
        .parquet(output_science)


if __name__ == "__main__":
    main()
