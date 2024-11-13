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
from pyspark.sql.window import Window
from pyspark.sql.functions import lit, collect_list


from fink_utils.spark.utils import concat_col
from fink_utils.xmatch.simbad import return_list_of_eg_host

from fink_broker.parser import getargs
from fink_broker.spark_utils import init_sparksession, load_parquet_files
from fink_broker.logging_utils import get_fink_logger, inspect_application

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
    to_expand = ["jd", "fid", "magpsf", "sigmapsf", "diffmaglim", "sigmapsf"]

    # Append temp columns with historical + current measurements
    prefix = "c"
    for colname in to_expand:
        df = concat_col(df, colname, prefix=prefix)

    # Use last limiting magnitude for feature extraction
    # Explode each column with `posexplode_outer` to include null values, generating the same `pos` index
    df_cfid = df.select("objectId", F.posexplode_outer("cfid").alias("pos", "cfid_exp"))
    df_cmagpsf = df.select(
        "objectId", F.posexplode_outer("cmagpsf").alias("pos", "cmagpsf_exp")
    )
    df_cjd = df.select("objectId", F.posexplode_outer("cjd").alias("pos", "cjd_exp"))
    df_csigmapsf = df.select(
        "objectId", F.posexplode_outer("csigmapsf").alias("pos", "csigmapsf_exp")
    )
    df_cdiffmaglim = df.select(
        "objectId", F.posexplode_outer("cdiffmaglim").alias("pos", "cdiffmaglim_exp")
    )

    # GET DETECTIONS
    # Join all exploded columns on `objectId` and `pos` to keep alignment
    df_dets_exploded = (
        df_cfid.join(df_cmagpsf, ["objectId", "pos"], "outer")
        .join(df_cjd, ["objectId", "pos"], "outer")
        .join(df_csigmapsf, ["objectId", "pos"], "outer")
        .drop("pos")  # Drop `pos` if you don't need it in the final result
    )
    # Get only valid detections
    df_dets_exploded = df_dets_exploded.dropDuplicates()
    df_dets_exploded = df_dets_exploded.filter(df_dets_exploded.cmagpsf_exp.isNotNull())
    # get minimum time and its corresponding magnitude per filter
    window_spec = Window.partitionBy("objectId", "cfid_exp").orderBy("cjd_exp")
    df_detection_min_cjd = (
        df_dets_exploded.withColumn("min_cjd_exp", F.first("cjd_exp").over(window_spec))
        .withColumn(
            "corresponding_cmagpsf_exp", F.first("cmagpsf_exp").over(window_spec)
        )
        .groupBy("objectId", "cfid_exp")
        .agg(
            F.min("min_cjd_exp").alias("min_cjd_exp"),
            F.first("corresponding_cmagpsf_exp").alias("corresponding_cmagpsf_exp"),
        )
    )

    # GET LIMITS
    df_lims_exploded = (
        df_cfid.join(df_cdiffmaglim, ["objectId", "pos"], "outer")
        .join(df_cjd, ["objectId", "pos"], "outer")
        .drop("pos")  # Drop `pos` if you don't need it in the final result
    )
    df_lims_exploded = df_lims_exploded.dropDuplicates()
    # Filter to find last limit fainter than detection
    df_filtered = df_lims_exploded.join(
        df_detection_min_cjd, on=["objectId", "cfid_exp"], how="inner"
    )
    df_filtered = df_filtered.filter(
        (df_filtered.cjd_exp < df_filtered.min_cjd_exp)
        & (df_filtered.cdiffmaglim_exp > df_filtered.corresponding_cmagpsf_exp)
    )
    df_filtered = df_filtered.select(
        "objectId", "cfid_exp", "cjd_exp", "cdiffmaglim_exp"
    )
    # Define a window specification partitioned by 'objectId' and 'cfid_exp', ordered by 'cjd_exp' descending
    window_spec = Window.partitionBy("objectId", "cfid_exp").orderBy(F.desc("cjd_exp"))
    # Add a row number to each row within the partition
    df_filtered_top_cjd = df_filtered.withColumn(
        "row_num", F.row_number().over(window_spec)
    )
    df_filtered_top_cjd = df_filtered_top_cjd.filter(F.col("row_num") == 1).drop(
        "row_num"
    )
    # Refactor column names
    df_limits = df_filtered_top_cjd.withColumnRenamed("cdiffmaglim_exp", "cmagpsf_exp")
    # add column for error, putting a large error 0.2
    df_limits = df_limits.withColumn("csigmapsf_exp", lit(0.2))

    # Append detections and limits
    df_appended = df_dets_exploded.unionByName(df_limits)
    df_aggregated = df_appended.groupBy("objectId").agg(
        collect_list("cfid_exp").alias("cfid_aggregated"),
        collect_list("cjd_exp").alias("cjd_aggregated"),
        collect_list("cmagpsf_exp").alias("cmagpsf_aggregated"),
        collect_list("csigmapsf_exp").alias("csigmapsf_aggregated"),
    )
    # Join the aggregated result back to the original DataFrame on "objectId"
    df_joined = df.join(df_aggregated, on="objectId", how="left")

    df = (
        df_joined.select(
            "*",  # Select all original columns
            "cfid_aggregated",
            "cjd_aggregated",
            "cmagpsf_aggregated",
            "csigmapsf_aggregated",  # Add the new aggregated columns
        )
        .drop(
            "cfid_exp",
            "cjd_exp",
            "cmagpsf_exp",
            "csigmapsf_exp",  # Drop the original exploded columns
            "cfid",
            "cjd",
            "cmagpsf",
            "csigmapsf",  # Drop the original columns that will be replaced
        )
        .withColumnRenamed("cfid_aggregated", "cfid")
        .withColumnRenamed("cjd_aggregated", "cjd")
        .withColumnRenamed("cmagpsf_aggregated", "cmagpsf")
        .withColumnRenamed("csigmapsf_aggregated", "csigmapsf")
    )

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
    model = curdir + "/data/models/for_al_loop/model_20240821.pkl"

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
        "candidate.jdstarthist",
        "candidate.jd",
    ]

    pdf = df_filt.select(cols_).toPandas()
    pdf = pdf.sort_values("dist_center", ascending=True)

    init_msg = f"Number of candidates for the night {args.night}: {len(pdf)} ({len(np.unique(pdf.objectId))} unique objects)."

    slack_data = []
    for _, row in pdf.head(30).iterrows():
        append_slack_messages(slack_data, row)

    msg_handler_slack(slack_data, "bot_al_loop", init_msg)

    # maximum 10 days between intial and final detection
    c7 = (pdf["jd"] - pdf["jdstarthist"]) <= 10.0
    pdf_early = pdf[c7]

    # Filter for high probabilities
    pdf_hp = pdf_early[pdf_early["al_snia_vs_nonia"] > 0.5]
    pdf_hp = pdf_hp.sort_values("al_snia_vs_nonia", ascending=False)

    init_msg = f"Number of candidates for the night {args.night} (high probability): {len(pdf_hp)} ({len(np.unique(pdf_hp.objectId))} unique objects)."

    slack_data = []
    for _, row in pdf_hp.head(30).iterrows():
        append_slack_messages(slack_data, row)

    msg_handler_slack(slack_data, "bot_al_loop_highprob", init_msg)


if __name__ == "__main__":
    main()
