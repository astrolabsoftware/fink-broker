#!/usr/bin/env python
# Copyright 2023-2024 AstroLab Software
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
"""Run the hostless detection module, and push data to Slack/Telegram."""

import argparse
import os

import numpy as np

from pyspark.sql import functions as F

from fink_utils.spark.utils import concat_col

from fink_broker.parser import getargs
from fink_broker.spark_utils import init_sparksession, load_parquet_files
from fink_broker.logging_utils import get_fink_logger, inspect_application

from fink_filters.classification import extract_fink_classification
from fink_filters.filter_anomaly_notification.filter_utils import msg_handler_slack
from fink_filters.filter_anomaly_notification.filter_utils import (
    get_data_permalink_slack,
)

from fink_science import __file__
from fink_science.hostless_detection.processor import run_potential_hostless


def append_slack_messages(slack_data: list, row: dict) -> None:
    """Append messages to list for Slack distribution.

    Parameters
    ----------
    slack_data: list
        List containing all Slack messages. Each element
        is a message (string).
    row: dict
        Pandas DataFrame row as dictionary. Contains
        Fink data.
    """
    t1 = f"ID: <https://fink-portal.org/{row.objectId}|{row.objectId}>"
    t2 = f"""
EQU: {row.ra},   {row.dec}"""

    t3 = f"Score: {round(row.kstest_static, 3)}"
    t4 = f"Fink classification: {row.finkclass}"
    cutout, curve, cutout_perml, curve_perml = get_data_permalink_slack(row.objectId)
    curve.seek(0)
    cutout.seek(0)
    cutout_perml = f"<{cutout_perml}|{' '}>"
    curve_perml = f"<{curve_perml}|{' '}>"
    slack_data.append(
        f"""==========================
{t1}
{t2}
{t3}
{t4}
{cutout_perml}{curve_perml}"""
    )


def main():
    """Extract probabilities from the hostless detection module, and send results to Slack."""
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(
        name="hostless_{}".format(args.night), shuffle_partitions=2
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

    # Retrieve time-series information
    to_expand = ["magpsf"]

    # Append temp columns with historical + current measurements
    prefix = "c"
    for colname in to_expand:
        df = concat_col(df, colname, prefix=prefix)

    # Add classification
    cols = [
        "cdsxmatch",
        "roid",
        "mulens",
        "snn_snia_vs_nonia",
        "snn_sn_vs_all",
        "rf_snia_vs_nonia",
        "candidate.ndethist",
        "candidate.drb",
        "candidate.classtar",
        "candidate.jd",
        "candidate.jdstarthist",
        "rf_kn_vs_nonkn",
        "tracklet",
    ]
    df = df.withColumn("finkclass", extract_fink_classification(*cols))

    # Add TNS classification -- fake for the moment
    df = df.withColumn("tnsclass", F.lit("Unknown"))

    # Add a new column
    df = df.withColumn(
        "kstest_static",
        run_potential_hostless(
            df["cmagpsf"],
            df["cutoutScience.stampData"],
            df["cutoutTemplate.stampData"],
            df["snn_snia_vs_nonia"],
            df["snn_sn_vs_all"],
            df["rf_snia_vs_nonia"],
            df["rf_kn_vs_nonkn"],
            df["finkclass"],
            df["tnsclass"],
        ),
    )

    cols_ = [
        "objectId",
        "candidate.ra",
        "candidate.dec",
        "kstest_static",
        "finkclass",
        "tnsclass",
    ]

    pdf = df.filter(df["kstest_static"] >= 0).select(cols_).toPandas()

    # log the number of candidates
    # sort by score

    init_msg = f"Number of candidates for the night {args.night}: {len(pdf)} ({len(np.unique(pdf.objectId))} unique objects)."

    slack_data = []
    # limit to the first 100th (guard against processing error)
    for _, row in pdf.head(30).iterrows():
        append_slack_messages(slack_data, row)

    msg_handler_slack(slack_data, "bot_hostless", init_msg)


if __name__ == "__main__":
    main()
