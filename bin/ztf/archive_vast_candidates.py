#!/usr/bin/env python
# Copyright 2026 AstroLab Software
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
"""Run VAST filters, and push data to Slack."""

import pyspark.sql.functions as F

import argparse

from fink_broker.common.parser import getargs
from fink_broker.common.spark_utils import init_sparksession, load_parquet_files
from fink_broker.common.logging_utils import get_fink_logger, inspect_application

from fink_filters.ztf.classification import extract_fink_classification
from fink_filters.ztf.filter_anomaly_notification.filter_utils import msg_handler_slack
from fink_filters.ztf.filter_anomaly_notification.filter_utils import (
    get_data_permalink_slack,
)

from fink_filters.ztf.filter_vast_supernovae.filter import vast_supernovae
from fink_filters.ztf.filter_vast_supernovae_candidates.filter import (
    vast_supernovae_candidates,
)


def append_slack_messages(slack_data: list, row: dict, slack_token_env: str) -> None:
    """Append messages to list for VAST Slack distribution.

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
    if row.tns == "":
        t0 = "Fink SN candidate"
    else:
        t0 = f"TNS classification: {row.tns}"

    t1 = f"Fink classification: {row.classification}"
    t2 = f"SN score: {round(row.snn_sn_vs_all, 3)}"
    t3 = f"Portal: <https://ztf.fink-portal.org/{row.objectId}|{row.objectId}>"

    cutout, curve, cutout_perml, curve_perml = get_data_permalink_slack(
        row.objectId, slack_token_env=slack_token_env
    )
    curve.seek(0)
    cutout.seek(0)
    cutout_perml = f"<{cutout_perml}|{' '}>"
    curve_perml = f"<{curve_perml}|{' '}>"
    slack_data.append(
        f"""==========================
{t0}
{t1}
{t2}
{t3}
{cutout_perml}{curve_perml}"""
    )


def main():
    """Filter alerts according to the VAST filters, and send results to Slack."""
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(name="VAST_{}".format(args.night), shuffle_partitions=2)

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

    df_vast = df.filter(
        vast_supernovae_candidates(
            F.col("mangrove.lum_dist").astype("float"),
            "candidate.dec",
            "snn_sn_vs_all",
        )
        | vast_supernovae(
            F.col("mangrove.lum_dist").astype("float"),
            "candidate.dec",
            "tns",
        )
    )

    cols_ = [
        "objectId",
        "candidate.dec",
        "classification",
        "snn_sn_vs_all",
        "tns",
    ]

    pdf_vast = df_vast.select(cols_).toPandas()

    init_msg = (
        f"Total number of candidates for the night {args.night}: {len(pdf_vast)}."
    )

    slack_token_env = "VAST_SLACK_TOKEN"
    slack_data = []
    for _, row in pdf_vast.iterrows():
        append_slack_messages(slack_data, row, slack_token_env=slack_token_env)
    msg_handler_slack(
        slack_data, "#fink_bot", init_msg, slack_token_env=slack_token_env
    )


if __name__ == "__main__":
    main()
