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
"""Run the AL loop, and push data to Slack."""

import argparse
import os

import numpy as np

from pyspark.sql import functions as F

from fink_utils.spark.utils import concat_col
from fink_utils.xmatch.simbad import return_list_of_eg_host

from fink_broker.parser import getargs
from fink_broker.sparkUtils import init_sparksession, load_parquet_files
from fink_broker.loggingUtils import get_fink_logger, inspect_application

from fink_filters.classification import extract_fink_classification
from fink_filters.filter_anomaly_notification.filter_utils import msg_handler_slack
from fink_filters.filter_anomaly_notification.filter_utils import (
    get_data_permalink_slack,
)

from fink_science import __file__
from fink_science.random_forest_snia.processor import rfscore_sigmoid_full


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

    t3 = f"Score: {round(row.al_snia_vs_nonia, 3)}"
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
    """Extract probabilities from the AL model, and send results to Slack."""
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(
        name="AL_loop_{}".format(args.night), shuffle_partitions=2
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
    to_expand = ["jd", "fid", "magpsf", "sigmapsf"]

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
    df = df.withColumn("classification", extract_fink_classification(*cols))

    # model path
    curdir = os.path.dirname(os.path.abspath(__file__))
    model = curdir + "/data/models/for_al_loop/model_20240422.pkl"

    # Run SN classification using AL model
    rfscore_args = ["cjd", "cfid", "cmagpsf", "csigmapsf"]
    rfscore_args += [F.col("cdsxmatch"), F.col("candidate.ndethist")]
    rfscore_args += [F.lit(1), F.lit(3), F.lit("diff")]
    rfscore_args += [F.lit(model)]
    df = df.withColumn("al_snia_vs_nonia", rfscore_sigmoid_full(*rfscore_args))

    # Distance to the middle of the distribution of scores
    df = df.withColumn("dist_center", F.abs(df["al_snia_vs_nonia"] - 0.5))

    # Filter
    c1 = df["cdsxmatch"].isin(return_list_of_eg_host().tolist())
    c2 = df["candidate.dec"] < 20.0
    c3 = (df["candidate.jd"] - df["candidate.jdstarthist"]) <= 20.0
    c4 = df["candidate.drb"] > 0.5
    c5 = df["candidate.classtar"] > 0.4
    c6 = df["al_snia_vs_nonia"] > 0.0

    df_filt = df.filter(c1).filter(c2).filter(c3).filter(c4).filter(c5).filter(c6)

    cols_ = [
        "objectId",
        "candidate.ra",
        "candidate.dec",
        "dist_center",
        "classification",
        "al_snia_vs_nonia",
        "rf_snia_vs_nonia",
        "candidate.ndethist",
    ]

    pdf = df_filt.select(cols_).toPandas()
    pdf = pdf.sort_values("dist_center", ascending=True)

    init_msg = f"Number of candidates for the night {args.night}: {len(pdf)} ({len(np.unique(pdf.objectId))} unique objects)."

    slack_data = []
    for _, row in pdf.head(30).iterrows():
        append_slack_messages(slack_data, row)

    msg_handler_slack(slack_data, "bot_al_loop", init_msg)

    # Filter for high probabilities
    pdf_hp = pdf[pdf["al_snia_vs_nonia"] > 0.5]
    pdf_hp = pdf_hp.sort_values("al_snia_vs_nonia", ascending=False)

    init_msg = f"Number of candidates for the night {args.night} (high probability): {len(pdf_hp)} ({len(np.unique(pdf_hp.objectId))} unique objects)."

    slack_data = []
    for _, row in pdf_hp.head(30).iterrows():
        append_slack_messages(slack_data, row)

    msg_handler_slack(slack_data, "bot_al_loop_highprob", init_msg)


if __name__ == "__main__":
    main()
