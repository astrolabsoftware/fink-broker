#!/usr/bin/env python
# Copyright 2019 AstroLab Software
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
"""Update the science database from the raw database alert data.

Step 1: Connect to the raw database
Step 2: Filter alerts based on instrumental or environmental criteria.
Step 3: Classify alerts using the xMatch service at CDS.
Step 4: Push alert data into the science database (TBD)

See http://cdsxmatch.u-strasbg.fr/ for more information on the SIMBAD catalog.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import argparse
import time

from fink_broker.sparkUtils import init_sparksession
from fink_broker.filters import keep_alert_based_on
from fink_broker.classification import cross_match_alerts_per_batch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        'outputpath', type=str,
        help='Directory on disk for saving live data. [FINK_ALERT_PATH]')
    parser.add_argument(
        '-exit_after', type=int, default=None,
        help=""" Stop the service after `exit_after` seconds.
        This primarily for use on Travis, to stop service after some time.
        Use that with `fink start service --exit_after <time>`.
        Default is None. """)
    args = parser.parse_args()

    # Grab the running Spark Session,
    # otherwise create it.
    spark = init_sparksession(
        name="filteringStream", shuffle_partitions=2, log_level="ERROR")

    # Create a DF from the database
    userschema = spark.read.format("parquet").load(args.outputpath).schema
    df = spark \
        .readStream \
        .format("parquet") \
        .schema(userschema) \
        .option("basePath", args.outputpath) \
        .option("path", args.outputpath + "/*") \
        .option("latestFirst", True) \
        .load()

    # Apply filters and keep only good alerts
    df_filt = df.withColumn(
        "toKeep",
        keep_alert_based_on(
            col("decoded.candidate.nbad"),
            col("decoded.candidate.rb"),
            col("decoded.candidate.magdiff")
        )
    ).filter("toKeep == true")

    # for good alerts, perform a cross-match with SIMBAD,
    # and return the types of the objects (Star, AGN, Unknown, etc.)
    df_type = df_filt.withColumn(
        "type",
        cross_match_alerts_per_batch(
            col("decoded.objectId"),
            col("decoded.candidate.ra"),
            col("decoded.candidate.dec")
        )
    )

    # Print the result on the screen.
    countquery = df_type\
        .writeStream\
        .outputMode("update") \
        .format("console").start()

    # Keep the Streaming running until something or someone ends it!
    if args.exit_after is not None:
        time.sleep(args.exit_after)
        countquery.stop()
        print("Exiting the archiving service normally...")
    else:
        countquery.awaitTermination()


if __name__ == "__main__":
    main()
