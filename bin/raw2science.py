#!/usr/bin/env python
# Copyright 2019-2020 AstroLab Software
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
"""Update the (tmp) science database from the raw database alert data.

Step 1: Connect to the raw database
Step 2: Filter alerts based on instrumental or environmental criteria.
Step 3: Run processors (aka science modules) on alerts to generate added value.
Step 4: Push alert data into the tmp science database (parquet)

See http://cdsxmatch.u-strasbg.fr/ for more information on the SIMBAD catalog.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import argparse
import time
import os

from fink_broker import __version__ as fbvsn
from fink_broker.parser import getargs
from fink_broker.sparkUtils import init_sparksession
from fink_broker.sparkUtils import connect_to_raw_database
from fink_broker.filters import apply_user_defined_filter
from fink_broker.loggingUtils import get_fink_logger, inspect_application

from fink_broker.science import apply_science_modules

from fink_science import __version__ as fsvsn

qualitycuts = 'fink_broker.filters.qualitycuts'

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(name="raw2science", shuffle_partitions=2)

    # Logger to print useful debug statements
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    df = connect_to_raw_database(
        args.rawdatapath, args.rawdatapath + "/*", latestfirst=False)

    # Apply quality cuts
    logger.info(qualitycuts)
    df = apply_user_defined_filter(df, qualitycuts)

    # Apply science modules
    df = apply_science_modules(df, logger)

    # Add library versions
    df = df.withColumn('fink_broker_version', F.lit(fbvsn))\
        .withColumn('fink_science_version', F.lit(fsvsn))

    # Switch publisher
    df = df.withColumn('publisher', F.lit('Fink'))

    # re-create partitioning columns.
    # Partitioned data doesn't preserve type information (cast as int...)
    df_partitionedby = df\
        .withColumn("year", F.date_format("timestamp", "yyyy"))\
        .withColumn("month", F.date_format("timestamp", "MM"))\
        .withColumn("day", F.date_format("timestamp", "dd"))

    # Append new rows in the tmp science database
    countquery = df_partitionedby\
        .writeStream\
        .outputMode("append") \
        .format("parquet") \
        .option("checkpointLocation", args.checkpointpath_sci_tmp) \
        .option("path", args.scitmpdatapath)\
        .partitionBy("year", "month", "day") \
        .start()

    # Keep the Streaming running until something or someone ends it!
    if args.exit_after is not None:
        time.sleep(args.exit_after)
        countquery.stop()
        logger.info("Exiting the raw2science service normally...")
    else:
        # Wait for the end of queries
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
