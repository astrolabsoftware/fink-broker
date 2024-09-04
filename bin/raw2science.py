#!/usr/bin/env python
# Copyright 2019-2024 AstroLab Software
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

from pyspark.sql import functions as F

import argparse
import time
import os

from fink_broker import __version__ as fbvsn
from fink_broker.logging_utils import init_logger
from fink_broker.parser import getargs
from fink_broker.spark_utils import init_sparksession
from fink_broker.spark_utils import connect_to_raw_database
from fink_broker.partitioning import convert_to_datetime, convert_to_millitime
from fink_broker.spark_utils import path_exist

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    if args.night == "elasticc":
        tz = "UTC"
    else:
        tz = None

    logger = init_logger(args.log_level)

    logger.debug("Initialise Spark session")
    spark = init_sparksession(
        name="raw2science_{}_{}".format(args.producer, args.night),
        shuffle_partitions=2,
        tz=tz,
        log_level=args.spark_log_level,
    )

    # data path
    rawdatapath = os.path.join(args.online_data_prefix, "raw")
    scitmpdatapath = os.path.join(
        args.online_data_prefix, "science/{}".format(args.night)
    )
    checkpointpath_sci_tmp = os.path.join(
        args.online_data_prefix, "science_checkpoint/{}".format(args.night)
    )

    if args.producer == "elasticc":
        df = connect_to_raw_database(rawdatapath, rawdatapath, latestfirst=False)
    else:
        # assume YYYYMMHH
        df = connect_to_raw_database(
            os.path.join(rawdatapath, "{}".format(args.night)),
            os.path.join(rawdatapath, "{}".format(args.night)),
            latestfirst=False,
        )

        # Add ingestion timestamp
        df = df.withColumn(
            "brokerStartProcessTimestamp",
            convert_to_millitime(df["candidate.jd"], F.lit("jd"), F.lit(True)),
        )

    # Add library versions
    if args.noscience:
        fsvsn = "no-science"
    else:
        # Do not import fink_science if --noscience is set
        from fink_science import __version__ as fsvsn


    df = df.withColumn("fink_broker_version", F.lit(fbvsn)).withColumn(
        "fink_science_version", F.lit(fsvsn)
    )

    logger.debug("Switch publisher")
    df = df.withColumn("publisher", F.lit("Fink"))

    logger.debug("Prepare and analyse the data")
    if "candidate" in df.columns:
        logger.info("Apply quality cuts")
        df = df.filter(df["candidate.nbad"] == 0).filter(df["candidate.rb"] >= 0.55)

        logger.debug("Discard an alert if it is in i band")
        df = df.filter(df["candidate.fid"] != 3)

        if args.noscience:
            logger.info("Do not apply science modules")
        else:
            # Do not import fink_science if --noscience is set
            from fink_science import apply_science_modules
            logger.info("Apply science modules")
            df = apply_science_modules(df)

        logger.debug("Add ingestion timestamp")
        df = df.withColumn(
            "brokerEndProcessTimestamp",
            convert_to_millitime(df["candidate.jd"], F.lit("jd"), F.lit(True)),
        )

        logger.debug("Append new rows in the tmp science database")
        countquery_science = (
            df.writeStream.outputMode("append")
            .format("parquet")
            .option("checkpointLocation", checkpointpath_sci_tmp)
            .option("path", scitmpdatapath)
            .trigger(processingTime="{} seconds".format(args.tinterval))
            .start()
        )

        if args.noscience:
            logger.info("Do not perform multi-messenger operations")
            time_spent_in_wait, countquery_mm = 0, None
        else:
            logger.debug("Perform multi-messenger operations")
            from fink_broker.mm_utils import launch_fink_mm
            time_spent_in_wait, countquery_mm = launch_fink_mm(args, scitmpdatapath)


        if args.exit_after is not None:
            logger.debug("Keep the Streaming running until something or someone ends it!")
            # If GCN arrived, wait for the remaining time since the launch of raw2science
            remaining_time = args.exit_after - time_spent_in_wait
            remaining_time = remaining_time if remaining_time > 0 else 0
            time.sleep(remaining_time)
            countquery_science.stop()
            if countquery_mm is not None:
                countquery_mm.stop()
        else:
            logger.debug("Wait for the end of queries")
            spark.streams.awaitAnyTermination()

    elif "diaSource" in df.columns:
        if args.noscience:
            logger.fatal("Elasticc data cannot be processed without science modules")
        else:
            from fink_science import apply_science_modules_elasticc
            logger.info("Apply elasticc science modules")
            df = apply_science_modules_elasticc(df)

        timecol = "diaSource.midPointTai"
        converter = lambda x: convert_to_datetime(x, F.lit("mjd"))

        logger.debug("Re-create partitioning columns if needed")
        if "timestamp" not in df.columns:
            df = df.withColumn("timestamp", converter(df[timecol]))

        if "year" not in df.columns:
            df = df.withColumn("year", F.date_format("timestamp", "yyyy"))

        if "month" not in df.columns:
            df = df.withColumn("month", F.date_format("timestamp", "MM"))

        if "day" not in df.columns:
            df = df.withColumn("day", F.date_format("timestamp", "dd"))

        logger.debug("Append new rows in the tmp science database")
        countquery = (
            df.writeStream.outputMode("append")
            .format("parquet")
            .option("checkpointLocation", checkpointpath_sci_tmp)
            .option("path", scitmpdatapath)
            .partitionBy("year", "month", "day")
            .trigger(processingTime="{} seconds".format(args.tinterval))
            .start()
        )

        if args.exit_after is not None:
            logger.debug("Keep the Streaming running until something or someone ends it!")
            time.sleep(args.exit_after)
            countquery.stop()
        else:
            logger.debug("Wait for the end of queries")
            spark.streams.awaitAnyTermination()

    logger.info("Exiting the raw2science service normally...")


if __name__ == "__main__":
    main()
