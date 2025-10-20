#!/usr/bin/env python
# Copyright 2025 AstroLab Software
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
"""Run the SLSN classifier, and push data to Slack."""

import argparse

import numpy as np

from fink_broker.common.parser import getargs
from fink_broker.common.spark_utils import init_sparksession, load_parquet_files
from fink_broker.common.logging_utils import get_fink_logger, inspect_application

from fink_filters.ztf.classification import extract_fink_classification
from fink_filters.ztf.filter_anomaly_notification.filter_utils import msg_handler_slack
from fink_filters.ztf.filter_anomaly_notification.filter_utils import (
    get_data_permalink_slack,
)


def append_slack_messages(slack_data: list, row: dict, slack_token_env: str) -> None:
    """Append messages to list for Slack distribution.

    Parameters
    ----------
    slack_data: list
        List containing all Slack messages. Each element
        is a message (string).
    row: dict
        Pandas DataFrame row as dictionary. Contains
        Fink data.
    slack_token_env: str
        Environment variable that has the Slack bot token
    """
    t1 = f"ID: <https://fink-portal.org/{row.objectId}|{row.objectId}>"
    t2 = f"""
EQU: {row.ra},   {row.dec}"""

    t3 = f"Score: {round(row.slsn_score, 3)}"
    t4 = f"Classification: {row.classification}"
    cutout, curve, cutout_perml, curve_perml = get_data_permalink_slack(
        row.objectId, slack_token_env=slack_token_env
    )
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
    """Extract probabilities from the SLSN model, and send results to Slack."""
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(name="SLSN_{}".format(args.night), shuffle_partitions=2)

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    # Connect to the aggregated science database
    path = "{}/science/year={}/month={}/day={}".format(
        args.agg_data_prefix, args.night[:4], args.night[4:6], args.night[6:8]
    )
    df = load_parquet_files(path)

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
    df = df.withColumn("classification", extract_fink_classification(*cols))

    # Simply filter at 0.5
    df_filt = df.filter(df["slsn_score"] >= 0.5)

    cols_ = [
        "objectId",
        "candidate.ra",
        "candidate.dec",
        "classification",
        "slsn_score",
        "candidate.ndethist",
        "candidate.jdstarthist",
        "candidate.jd",
    ]

    pdf = df_filt.select(cols_).toPandas()
    pdf = pdf.sort_values("slsn_score", ascending=False)

    init_msg = f"Number of candidates for the night {args.night}: {len(pdf)} ({len(np.unique(pdf.objectId))} unique objects)."
    print(init_msg)

    envs = ["ANOMALY_SLACK_TOKEN", "SLSN_SLACK_ZTF", "SLSN_SLACK_OSCAR"]
    channels = ["#bot_slsn", "#slsn-candidates", "#slsn-candidates"]
    for slack_token_env, channel in zip(envs, channels):
        slack_data = []
        for _, row in pdf.iterrows():
            append_slack_messages(slack_data, row, slack_token_env)

        msg_handler_slack(
            slack_data, channel, init_msg, slack_token_env=slack_token_env
        )


if __name__ == "__main__":
    main()
