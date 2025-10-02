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

from pyspark.sql import functions as F


from fink_broker.common.parser import getargs
from fink_broker.common.spark_utils import init_sparksession, load_parquet_files
from fink_broker.common.logging_utils import get_fink_logger, inspect_application

from fink_science.ztf.superluminous.processor import superluminous_score
from fink_filters.ztf.filter_superluminous.filter import slsn_filter

from fink_filters.ztf.classification import extract_fink_classification
from fink_filters.ztf.filter_anomaly_notification.filter_utils import (
    get_data_permalink_slack,
)


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

    t3 = f"Score: {round(row.slsn_score, 3)}"
    t4 = f"Classification: {row.classification}"
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

    # Run SLSN classification
    args = ["is_transient", "objectId", "candidate.jd", "candidate.jdstarthist"]
    df = df.withColumn("slsn_score", superluminous_score(*args))

    # Filter at 0.5
    df = df.filter(slsn_filter("slsn_score", F.lit(0.5)))

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

    pdf = df.select(cols_).toPandas()
    pdf = pdf.sort_values("slsn_score", ascending=False)

    init_msg = f"Number of candidates for the night {args.night}: {len(pdf)} ({len(np.unique(pdf.objectId))} unique objects)."
    print(init_msg)

    # slack_data = []
    # for _, row in pdf.iterrows():
    #     append_slack_messages(slack_data, row)

    # msg_handler_slack(slack_data, "XXXXX", init_msg)


if __name__ == "__main__":
    main()
