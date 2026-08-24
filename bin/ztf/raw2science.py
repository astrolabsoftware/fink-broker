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
import sys
import time
import os

from fink_broker import __version__ as fbvsn
from fink_broker.common.logging_utils import init_logger
from fink_broker.common.parser import getargs
from fink_broker.common.night_utils import get_exit_deadline, seconds_until
from fink_broker.common.spark_utils import init_sparksession
from fink_broker.common.spark_utils import (
    NoDataAvailableError,
    connect_to_raw_database,
)
from fink_broker.common.spark_utils import wait_for_filesystem
from fink_broker.common.partitioning import convert_to_millitime


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    logger = init_logger(args.log_level)

    # Anchored on the day the job starts, so a restart aims at the same
    # instant instead of granting itself a fresh window.
    exit_deadline = get_exit_deadline(args.exit_at)

    logger.debug("Initialise Spark session")
    spark = init_sparksession(
        name="raw2science_{}_{}".format(args.producer, args.night),
        shuffle_partitions=2,
        tz=None,
        log_level=args.spark_log_level,
    )

    # This job is deployed alongside the services it depends on, so they may
    # not be up yet. Wait for them instead of failing on the first access.
    wait_for_filesystem(args.online_data_prefix)

    # data path
    rawdatapath = os.path.join(args.online_data_prefix, "raw")
    scitmpdatapath = os.path.join(
        args.online_data_prefix, "science/{}".format(args.night)
    )
    checkpointpath_sci_tmp = os.path.join(
        args.online_data_prefix, "science_checkpoint/{}".format(args.night)
    )

    # assume YYYYMMHH
    try:
        df = connect_to_raw_database(
            os.path.join(rawdatapath, "{}".format(args.night)),
            os.path.join(rawdatapath, "{}".format(args.night)),
            latestfirst=False,
        )
    except NoDataAvailableError as e:
        # The telescope does not observe every night. Exit successfully so the
        # scheduler frees the slot instead of retrying a job with no input.
        logger.info(
            "No alert collected for night %s, nothing to do (%s)", args.night, e
        )
        return

    # Add ingestion timestamp
    df = df.withColumn(
        "brokerStartProcessTimestamp",
        convert_to_millitime(df["candidate.jd"], "jd", True),
    )

    # Add library versions
    if args.noscience:
        logger.debug("Do not import fink_science because --noscience is set")
        fsvsn = "no-science"
    else:
        from fink_science import __version__ as fsvsn

    logger.info("Fink broker {} - Fink science {}".format(fbvsn, fsvsn))
    df = df.withColumn("fink_broker_version", F.lit(fbvsn))
    df = df.withColumn("fink_science_version", F.lit(fsvsn))

    logger.debug("Switch publisher")
    df = df.withColumn("publisher", F.lit("Fink"))

    logger.debug("Prepare and analyse the data")
    logger.info("Apply quality cuts")
    df = df.filter(df["candidate.nbad"] == 0).filter(df["candidate.rb"] >= 0.55)

    logger.debug("Discard an alert if it is in i band")
    df = df.filter(df["candidate.fid"] != 3)

    if args.noscience:
        logger.info("Do not apply science modules")
    else:
        logger.info("Import science modules")
        from fink_broker.ztf.science import apply_science_modules

        logger.info("Apply science modules")
        df = apply_science_modules(df, args.tns_raw_output)

    logger.debug("Add ingestion timestamp")
    df = df.withColumn(
        "brokerEndProcessTimestamp",
        convert_to_millitime(df["candidate.jd"], "jd", True),
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
        logger.info("Perform multi-messenger operations")
        from fink_broker.ztf.mm_utils import raw2science_launch_fink_mm

        time_spent_in_wait, countquery_mm = raw2science_launch_fink_mm(
            args, scitmpdatapath
        )

    if exit_deadline is not None:
        # An absolute deadline already accounts for whatever was spent waiting
        # for the upstream data or for a GCN, so nothing is subtracted here.
        logger.debug("Keep the Streaming until the exit_at deadline %s", exit_deadline)
        time.sleep(seconds_until(exit_deadline))
        countquery_science.stop()
        if countquery_mm is not None:
            countquery_mm.stop()
        logger.warning(
            "Reached the exit_at deadline %s: some alerts of night %s may not "
            "have been processed",
            exit_deadline,
            args.night,
        )
        sys.exit(1)
    elif args.exit_after is not None:
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

    logger.info("Exiting the raw2science service normally...")


if __name__ == "__main__":
    main()
