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
import logging
import time
import os

from fink_broker import __version__ as fbvsn
from fink_broker.parser import getargs
from fink_broker.spark_utils import init_sparksession
from fink_broker.spark_utils import connect_to_raw_database
from fink_broker.partitioning import convert_to_datetime, convert_to_millitime
from fink_broker.spark_utils import path_exist

from fink_broker.science import apply_science_modules
from fink_broker.science import apply_science_modules_elasticc

from fink_science import __version__ as fsvsn

_LOG = logging.getLogger(__name__)


def launch_fink_mm(args: dict, scitmpdatapath: str):
    """Manage multimessenger operations

    Parameters
    ----------
    args: dict
        Arguments from Fink configuration file
    scitmpdatapath: str
        Path to Fink alert data (science)

    Returns
    -------
    time_spent_in_wait: int
        Time spent in waiting for GCN to come
        before launching the streaming query.
    countquery_mm: StreamingQuery
        Spark Streaming query

    """
    if args.noscience:
        _LOG.info("No science: fink-mm is not applied")
        return 0, None
    elif args.mmconfigpath != "no-config":
        from fink_mm.init import get_config
        from fink_broker.mm_utils import science2mm

        _LOG.info("Fink-MM configuration file: {args.mmconfigpath}")
        config = get_config({"--config": args.mmconfigpath})
        gcndatapath = config["PATH"]["online_gcn_data_prefix"]
        gcn_path = gcndatapath + "/year={}/month={}/day={}".format(
            args.night[0:4], args.night[4:6], args.night[6:8]
        )

        # Wait for GCN comming
        time_spent_in_wait = 0
        while time_spent_in_wait < args.exit_after:
            # if there is gcn and ztf data
            if path_exist(gcn_path) and path_exist(scitmpdatapath):
                # Start the GCN x ZTF cross-match stream
                t_before = time.time()
                _LOG.info("starting science2mm ...")
                countquery_mm = science2mm(args, config, gcn_path, scitmpdatapath)
                time_spent_in_wait += time.time() - t_before
                break
            else:
                # wait for comming GCN
                time_spent_in_wait += 1
                time.sleep(1)
        _LOG.info("Time spent in waiting for Fink-MM: {time_spent_in_wait} seconds")
        return time_spent_in_wait, countquery_mm

    _LOG.info("No configuration found for fink-mm -- no applied")
    return 0, None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    if args.night == "elasticc":
        tz = "UTC"
    else:
        tz = None

    # Initialise Spark session
    spark = init_sparksession(
        name="raw2science_{}_{}".format(args.producer, args.night),
        shuffle_partitions=2,
        tz=tz,
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
    df = df.withColumn("fink_broker_version", F.lit(fbvsn)).withColumn(
        "fink_science_version", F.lit(fsvsn)
    )

    # Switch publisher
    df = df.withColumn("publisher", F.lit("Fink"))

    # Apply science modules
    if "candidate" in df.columns:
        # Apply quality cuts
        _LOG.info("Applying quality cuts")
        df = df.filter(df["candidate.nbad"] == 0).filter(df["candidate.rb"] >= 0.55)

        # Discard an alert if it is in i band
        # or if it contains i band measurements in history
        df = df.filter(df["candidate.fid"] != 3)
        df = df.filter(~F.array_contains(df["prv_candidates.fid"], 3))

        # Apply science modules
        _LOG.info("Applying science modules")
        df = apply_science_modules(df, args.noscience)

        # Add ingestion timestamp
        df = df.withColumn(
            "brokerEndProcessTimestamp",
            convert_to_millitime(df["candidate.jd"], F.lit("jd"), F.lit(True)),
        )

        # Append new rows in the tmp science database
        countquery_science = (
            df.writeStream.outputMode("append")
            .format("parquet")
            .option("checkpointLocation", checkpointpath_sci_tmp)
            .option("path", scitmpdatapath)
            .trigger(processingTime="{} seconds".format(args.tinterval))
            .start()
        )

        # Perform multi-messenger operations
        time_spent_in_wait, countquery_mm = launch_fink_mm(args, scitmpdatapath)

        # Keep the Streaming running until something or someone ends it!
        if args.exit_after is not None:
            # If GCN arrived, wait for the remaining time since the launch of raw2science
            remaining_time = args.exit_after - time_spent_in_wait
            remaining_time = remaining_time if remaining_time > 0 else 0
            time.sleep(remaining_time)
            countquery_science.stop()
            if countquery_mm is not None:
                countquery_mm.stop()
        else:
            # Wait for the end of queries
            spark.streams.awaitAnyTermination()

    elif "diaSource" in df.columns:
        df = apply_science_modules_elasticc(df)
        timecol = "diaSource.midPointTai"
        converter = lambda x: convert_to_datetime(x, F.lit("mjd"))

        # re-create partitioning columns if needed.
        if "timestamp" not in df.columns:
            df = df.withColumn("timestamp", converter(df[timecol]))

        if "year" not in df.columns:
            df = df.withColumn("year", F.date_format("timestamp", "yyyy"))

        if "month" not in df.columns:
            df = df.withColumn("month", F.date_format("timestamp", "MM"))

        if "day" not in df.columns:
            df = df.withColumn("day", F.date_format("timestamp", "dd"))

        # Append new rows in the tmp science database
        countquery = (
            df.writeStream.outputMode("append")
            .format("parquet")
            .option("checkpointLocation", checkpointpath_sci_tmp)
            .option("path", scitmpdatapath)
            .partitionBy("year", "month", "day")
            .trigger(processingTime="{} seconds".format(args.tinterval))
            .start()
        )

        # Keep the Streaming running until something or someone ends it!
        if args.exit_after is not None:
            time.sleep(args.exit_after)
            countquery.stop()
        else:
            # Wait for the end of queries
            spark.streams.awaitAnyTermination()

    _LOG.info("Exiting the raw2science service normally...")


if __name__ == "__main__":
    main()
