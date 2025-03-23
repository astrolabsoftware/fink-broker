#!/usr/bin/env python
# Copyright 2024 AstroLab Software
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
"""Run the xmatch with Dwarf AGN, and push data to Slack"""

import argparse

from pyspark.sql import functions as F

from fink_broker.common.parser import getargs
from fink_broker.common.spark_utils import init_sparksession, load_parquet_files
from fink_broker.common.logging_utils import get_fink_logger, inspect_application

from fink_filters.filter_anomaly_notification.filter_utils import msg_handler_slack
from fink_filters.filter_anomaly_notification.filter_utils import (
    get_data_permalink_slack,
)

from fink_filters.filter_dwarf_agn.filter import crossmatch_dwarf_agn


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(
        name="dwarf_AGN{}".format(args.night), shuffle_partitions=2
    )

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    # Connect to the aggregated science database
    path = "{}/science/year={}/month={}/day={}".format(
        args.agg_data_prefix, args.night[:4], args.night[4:6], args.night[6:8]
    )
    df = load_parquet_files(path)

    # Remove known asteroids
    df = df.filter(df["roid"] != 3)

    args_func = ["candidate.candid", "candidate.ra", "candidate.dec"]
    pdf = (
        df.withColumn("manga", crossmatch_dwarf_agn(*args_func))
        .filter(F.col("manga") != "Unknown")
        .select(["objectId", "manga"] + args_func)
        .toPandas()
    )

    if not pdf.empty:
        init_msg = "New association!"

        slack_data = []
        for _, row in pdf.iterrows():
            t1 = f"{row.manga}: <https://fink-portal.org/{row.objectId}|{row.objectId}>"

            # if you need lightcurve, etc.
            cutout, curve, cutout_perml, curve_perml = get_data_permalink_slack(
                row.objectId
            )
            curve.seek(0)
            cutout.seek(0)
            cutout_perml = f"<{cutout_perml}|{' '}>"
            curve_perml = f"<{curve_perml}|{' '}>"
            slack_data.append(f"""{t1}\n{cutout_perml}{curve_perml}""")

        msg_handler_slack(slack_data, "bot_manga", init_msg)
    else:
        msg_handler_slack([], "bot_manga", "{}: no associations".format(args.night))


if __name__ == "__main__":
    main()
