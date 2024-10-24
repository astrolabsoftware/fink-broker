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
"""Run the hostless detection module, and push data to Slack/Telegram."""

import argparse
import os
import pandas as pd

from pyspark.sql import functions as F

from fink_utils.spark.utils import concat_col

from fink_broker.parser import getargs
from fink_broker.spark_utils import init_sparksession, load_parquet_files
from fink_broker.logging_utils import get_fink_logger, inspect_application

from fink_filters.classification import extract_fink_classification

from fink_utils.tg_bot.utils import get_curve
from fink_utils.tg_bot.utils import get_cutout
from fink_utils.tg_bot.utils import msg_handler_tg_cutouts

from fink_tns.utils import read_past_ids

from fink_science.hostless_detection.processor import run_potential_hostless


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
            df["candidate.jd"] - df["candidate.jdstarthist"],
        ),
    )

    cols_ = [
        "objectId",
        "candidate.ra",
        "candidate.dec",
        "kstest_static",
        "finkclass",
        "tnsclass",
        F.col("cutoutScience.stampData").alias("cutoutScience"),
        F.col("cutoutTemplate.stampData").alias("cutoutTemplate"),
    ]

    cond_science_low = df["kstest_static"][0] >= 0.
    cond_science_high = df["kstest_static"][0] <= 0.5
    cond_template_low = df["kstest_static"][1] >= 0.
    cond_template_high = df["kstest_static"][1] <= 0.85

    pdf = df\
        .filter(cond_science_low & cond_science_high)\
        .filter(cond_template_low & cond_template_high)\
        .select(cols_)\
        .toPandas()

    # load hostless IDs
    past_ids = read_past_ids(args.hostless_folder)

    new_ids = []
    # Loop over matches & send to Telegram
    if ("FINK_TG_TOKEN" in os.environ) and os.environ["FINK_TG_TOKEN"] != "":
        payloads = []
        for _, alert in pdf.iterrows():
            # Do not send request if the object
            # has been already reported by the bot
            if alert["objectId"] in past_ids.to_numpy():
                print("{} already sent!".format(alert["objectId"]))
                continue

            curve_png = get_curve(
                objectId=alert["objectId"],
                origin="API",
            )

            cutout_science = get_cutout(cutout=alert["cutoutScience"])
            cutout_template = get_cutout(cutout=alert["cutoutTemplate"])

            text = """
*Object ID*: [{}](https://fink-portal.org/{})
*Scores:*\n- Science: {:.2f}\n- Template: {:.2f}
*Fink class*: {}
            """.format(
                alert["objectId"],
                alert["objectId"],
                alert["kstest_static"][0],
                alert["kstest_static"][1],
                alert["finkclass"],
            )

            payloads.append((text, curve_png, [cutout_science, cutout_template]))
            new_ids.append(alert["objectId"])

        if len(payloads) > 0:
            # Send to tg
            msg_handler_tg_cutouts(payloads, channel_id="@fink_hostless", init_msg="")

            # Save ids on disk
            pdf_ids = pd.DataFrame.from_dict({"id": new_ids})
            name = "{}{}{}".format(args.night[:4], args.night[4:6], args.night[6:8])
            pdf_ids.to_csv("{}/{}.csv".format(args.hostless_folder, name), index=False)


if __name__ == "__main__":
    main()
