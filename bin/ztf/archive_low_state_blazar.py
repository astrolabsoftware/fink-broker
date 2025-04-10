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
"""Run the blazar filters, and push data to HBase and Telegram."""

from pyspark.sql import functions as F

import logging
import argparse
import os

import numpy as np
from astropy.time import Time

from fink_broker.common.parser import getargs
from fink_broker.common.spark_utils import init_sparksession, load_parquet_files
from fink_broker.common.logging_utils import get_fink_logger, inspect_application
from fink_broker.ztf.hbase_utils import push_full_df_to_hbase

from fink_filters.ztf.filter_blazar_low_state.filter import low_state_filter
from fink_filters.ztf.filter_blazar_new_low_state.filter import new_low_state_filter

from fink_science.ztf.standardized_flux.processor import standardized_flux

from fink_utils.spark.utils import concat_col
from fink_utils.tg_bot.utils import send_simple_text_tg
from fink_utils.tg_bot.utils import get_curve
from fink_utils.tg_bot.utils import msg_handler_tg

_LOG = logging.getLogger(__name__)


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
            threshold = alert["flux"][-1] / alert["m2"]
            curve_png = get_curve(
                jd=alert["cjd"],
                magpsf=alert["flux"],
                sigmapsf=alert["sigma"],
                diffmaglim=np.array([None] * len(alert["flux"])),
                fid=alert["cfid"],
                objectId=alert["objectId"],
                origin="fields",
                ylabel="Standardized flux",
                title="{} - {}".format(
                    alert["objectId"], Time(alert["cjd"][-1], format="jd").iso
                ),
                invert_yaxis=False,
                hline={"y": threshold, "y_label": "Fq"},
            )

            text = """
*Object ID*: [{}](https://fink-portal.org/{})
            """.format(
                alert["objectId"],
                alert["objectId"],
            )

            payloads.append((text, None, curve_png))

        if len(payloads) > 0:
            # Send to tg
            msg_handler_tg(payloads, channel_id=channel, init_msg="")
    else:
        print("Telegram token FINK_TG_TOKEN is not set")


def get_std_flux(df):
    """ """
    # Retrieve time-series information
    to_expand = [
        "jd",
        "fid",
        "magpsf",
        "sigmapsf",
        "magnr",
        "sigmagnr",
        "isdiffpos",
        "distnr",
        "diffmaglim",
    ]

    # Append temp columns with historical + current measurements
    prefix = "c"
    for colname in to_expand:
        df = concat_col(df, colname, prefix=prefix)

    standardisation_args = [
        "candid",
        "objectId",
        "cdistnr",
        "cmagpsf",
        "csigmapsf",
        "cmagnr",
        "csigmagnr",
        "cisdiffpos",
        "cfid",
        "cjd",
    ]
    df = df.withColumn("container", standardized_flux(*standardisation_args))
    return df


def main():
    """Run the blazar filters and send results to HBase and telegram"""
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(name="blazar_{}".format(args.night), shuffle_partitions=2)

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    # Connect to the aggregated science database
    path = "{}/science/year={}/month={}/day={}".format(
        args.agg_data_prefix, args.night[:4], args.night[4:6], args.night[6:8]
    )
    df = load_parquet_files(path)

    # Drop partitioning columns
    df = df.drop("year").drop("month").drop("day")

    # Filter for low states in general (incl. new ones)
    df_new_low_state = df.filter(
        new_low_state_filter("blazar_stats.m0", "blazar_stats.m1", "blazar_stats.m2")
    )

    # Recompute std flux for plots
    df_new_low_state = get_std_flux(df_new_low_state)

    # Telegram -- send only new low states
    args_filter = [
        "objectId",
        "cjd",
        F.col("container").getItem("flux").alias("flux"),
        F.col("container").getItem("sigma").alias("sigma"),
        "cfid",
        "blazar_stats.m2",
    ]
    pdf = df_new_low_state.select(args_filter).toPandas()
    pdf["flux"] = pdf["flux"].apply(lambda x: np.array(x))
    pdf["sigma"] = pdf["sigma"].apply(lambda x: np.array(x))
    pdf["cjd"] = pdf["cjd"].apply(lambda x: np.array(x))
    pdf["cfid"] = pdf["cfid"].apply(lambda x: np.array(x))

    if not pdf.empty:
        _LOG.info("{} source(s) passing in low state")
        send_to_telegram(pdf, channel="@fink_blazar_new_low_states")
    else:
        send_simple_text_tg(
            "No matches for {}".format(args.night),
            channel_id="@fink_blazar_new_low_states",
        )

    # HBase -- Filter for low states in general (incl. new ones)
    # Drop images
    df = df.drop("cutoutScience").drop("cutoutTemplate").drop("cutoutDifference")
    df_low_state = df.filter(
        low_state_filter("blazar_stats.m1", "blazar_stats.m2")
    ).cache()

    n_low_state = df_low_state.count()
    _LOG.info("{} new entries".format(n_low_state))

    if n_low_state > 0:
        # Row key
        row_key_name = "jd_objectId"

        # push data to HBase
        push_full_df_to_hbase(
            df_low_state,
            row_key_name=row_key_name,
            table_name=args.science_db_name + ".low_state_blazars",
            catalog_name=args.science_db_catalogs,
        )


if __name__ == "__main__":
    main()
