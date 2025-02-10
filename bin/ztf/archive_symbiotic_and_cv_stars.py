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
"""Run the symbiotic & CV filters, and push data to Telegram."""

import argparse
import os


from fink_broker.ztf.parser import getargs
from fink_broker.common.spark_utils import init_sparksession, load_parquet_files
from fink_broker.common.logging_utils import get_fink_logger, inspect_application

from fink_filters.filter_symbiotic_stars.filter import crossmatch_symbiotic

from fink_utils.tg_bot.utils import get_curve
from fink_utils.tg_bot.utils import msg_handler_tg_cutouts


def send_to_telegram(pdf, channel):
    """Send candidates to TG channel

    Parameters
    ----------
    pdf: pd.DataFrame
        Pandas DataFrame with Fink data
    channel: str
        TG channel name to send candidates to. Must exist.
    """
    # Loop over matches & send to Telegram
    if ("FINK_TG_TOKEN" in os.environ) and os.environ["FINK_TG_TOKEN"] != "":
        payloads = []
        for _, alert in pdf.iterrows():
            curve_png = get_curve(
                objectId=alert["objectId"],
                origin="API",
            )

            text = """
*Object ID*: [{}](https://fink-portal.org/{})
*Name*: {}
*RA/Dec coordinates*: {} {}
*Mag difference*: {:.2f} ({:.2f} days in between measurements)
            """.format(
                alert["objectId"],
                alert["objectId"],
                alert["name"],
                alert["ra"],
                alert["dec"],
                alert["dmag"],
                alert["delta_time"],
            )

            payloads.append((text, curve_png, None))

        if len(payloads) > 0:
            # Send to tg
            msg_handler_tg_cutouts(payloads, channel_id=channel, init_msg="")
    else:
        print("Telegram token FINK_TG_TOKEN is not set")


def main():
    """Run the symbiotic and CVs filters and send results to telegram"""
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(
        name="symbiotic_{}".format(args.night), shuffle_partitions=2
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

    args = ["candidate.ra", "candidate.dec"]
    df = df.withColumn("symbiotic", crossmatch_symbiotic(*args))

    df = df.filter(df["symbiotic"] != "Unknown")

    # For the Symbiotic stars bot we should filter all alerts
    # with delta mag between the last 2 points bigger than 0.5 (any filter).
    pdf = (
        df.filter(df["mag_rate"] * df["delta_time"] <= -0.5)
        .filter(~df["from_upper"])
        .select([
            "candidate.ra",
            "candidate.dec",
            "symbiotic",
            "objectId",
            "delta_time",
            (df["mag_rate"] * df["delta_time"]).alias("dmag"),
        ])
        .toPandas()
    )

    if not pdf.empty:
        pdf["name"] = pdf["symbiotic"].apply(lambda x: x.split(",")[0])
        pdf["cat"] = pdf["symbiotic"].apply(lambda x: x.split(",")[1])

        pdf_sym = pdf[pdf["cat"] == "symbiotic_stars"]
        send_to_telegram(pdf_sym, channel="@fink_symbiotic_stars")

        pdf_cvs = pdf[pdf["cat"] == "cataclysmic_variables"]
        # CVs have stringent criterion
        pdf_cvs = pdf_cvs[pdf_cvs["dmag"] <= -3.0]
        send_to_telegram(pdf_cvs, channel="@fink_cv_stars")


if __name__ == "__main__":
    main()
