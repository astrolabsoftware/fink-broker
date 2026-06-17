#!/usr/bin/env python
# Copyright 2019-2025 AstroLab Software
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
from fink_broker.common.logging_utils import init_logger
from fink_broker.common.spark_utils import init_sparksession
from fink_broker.common.spark_utils import connect_to_raw_database
from fink_broker.common.partitioning import convert_to_millitime
from fink_broker.common.parser import getargs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    logger = init_logger(args.log_level)

    logger.debug("Initialise Spark session")
    spark = init_sparksession(
        name="raw2science_{}_{}".format(args.producer, args.night),
        shuffle_partitions=2,
        tz=None,
        log_level=args.spark_log_level,
    )

    # data path
    rawdatapath = os.path.join(args.online_data_prefix, "raw")
    scitmpdatapath = os.path.join(
        args.online_data_prefix,
        "science/{}".format(args.night),
    )
    checkpointpath_sci_tmp = os.path.join(
        args.online_data_prefix, "science_checkpoint/{}".format(args.night)
    )

    # assume YYYYMMHH
    df = connect_to_raw_database(
        os.path.join(rawdatapath, "{}".format(args.night)),
        os.path.join(rawdatapath, "{}".format(args.night)),
        latestfirst=False,
    )

    # Add ingestion timestamp
    df = df.withColumn(
        "brokerStartProcessTimestamp",
        convert_to_millitime(df["diaSource.midpointMjdTai"], F.lit("mjd"), F.lit(True)),
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

    logger.debug("Publisher")
    df = df.withColumn("publisher", F.lit("Fink"))

    logger.debug("Prepare and analyse the data")
    if args.noscience:
        logger.info("Do not apply science modules")
    else:
        from fink_broker.rubin.science import apply_science_modules

        logger.info("Apply Rubin science modules")
        df = apply_science_modules(df)

    logger.debug("Add ingestion timestamp")
    df = df.withColumn(
        "brokerEndProcessTimestamp",
        convert_to_millitime(df["diaSource.midpointMjdTai"], F.lit("mjd"), F.lit(True)),
    )

    logger.debug("Append new rows in the tmp science data lake")
    countquery = (
        df.writeStream.outputMode("append")
        .format("parquet")
        .option("checkpointLocation", checkpointpath_sci_tmp)
        .option("path", scitmpdatapath)
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
