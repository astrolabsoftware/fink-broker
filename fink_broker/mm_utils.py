# Copyright 2024 AstroLab Software
# Author: Roman Le Montagner
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
"""Utilities for multi-messenger activities"""

from fink_mm.ztf_join_gcn import ztf_join_gcn_stream, DataMode
from fink_mm.distribution.distribution import grb_distribution_stream

from astropy.time import Time
from datetime import timedelta

from fink_broker.spark_utils import connect_to_raw_database
from fink_broker.logging_utils import get_fink_logger

from pyspark.sql.streaming import StreamingQuery

import argparse
import configparser
import os
import time


def science2mm(
    args: argparse.Namespace,
    config: configparser.ConfigParser,
    gcndatapath: str,
    scitmpdatapath: str,
) -> StreamingQuery:
    """
    Launch the GCN x ZTF cross-match pipeline.

    Parameters
    ----------
    args : argparse.Namespace
        arguments from the fink command line
    config : configparser.ConfigParser
        config entries of fink_mm
    gcndatapath : str
        path where are stored the GCN x ZTF data
    scitmpdatapath : str
        path where are stored the fink science stream

    Returns
    -------
    StreamingQuery
        the fink_mm query, used by the caller
    """
    logger = get_fink_logger()
    wait = 5
    while True:
        try:
            ztf_dataframe = connect_to_raw_database(
                scitmpdatapath, scitmpdatapath, latestfirst=False
            )
            gcn_dataframe = connect_to_raw_database(
                gcndatapath,
                gcndatapath,
                latestfirst=False,
            )
            logger.info("successfully connect to the fink science and gcn database")
            break

        except Exception:
            logger.info("Exception occured: wait: {}".format(wait), exc_info=1)
            time.sleep(wait)
            wait *= 1.2 if wait < 60 else 1
            continue

    # keep gcn emitted between the last day time and the end of the current stream (17:00 Paris Time)
    cur_time = Time(f"{args.night[0:4]}-{args.night[4:6]}-{args.night[6:8]}")
    last_time = cur_time - timedelta(hours=7)  # 17:00 Paris time yesterday
    end_time = cur_time + timedelta(hours=17)  # 17:00 Paris time today
    gcn_dataframe = gcn_dataframe.filter(
        "triggerTimejd >= {} and triggerTimejd < {}".format(last_time.jd, end_time.jd)
    )
    df_multi_messenger, _ = ztf_join_gcn_stream(
        DataMode.STREAMING,
        ztf_dataframe,
        gcn_dataframe,
        gcndatapath,
        args.night,
        int(config["ADMIN"]["NSIDE"]),
        config["HDFS"]["host"],
        ast_dist=float(config["PRIOR_FILTER"]["ast_dist"]),
        pansstar_dist=float(config["PRIOR_FILTER"]["pansstar_dist"]),
        pansstar_star_score=float(config["PRIOR_FILTER"]["pansstar_star_score"]),
        gaia_dist=float(config["PRIOR_FILTER"]["gaia_dist"]),
    )

    mm_path_output = config["PATH"]["online_grb_data_prefix"]
    mmtmpdatapath = os.path.join(mm_path_output, "online")
    checkpointpath_mm_tmp = os.path.join(mm_path_output, "online_checkpoint")

    query_mm = (
        df_multi_messenger.writeStream.outputMode("append")
        .format("parquet")
        .option("checkpointLocation", checkpointpath_mm_tmp)
        .option("path", mmtmpdatapath)
        .partitionBy("year", "month", "day")
        .trigger(processingTime="{} seconds".format(args.tinterval))
        .start()
    )

    return query_mm


def mm2distribute(spark, config: configparser.ConfigParser, args: argparse.Namespace):
    """Launch the streaming between ZTF and GCN streams"""
    mm_data_path = config["PATH"]["online_grb_data_prefix"]
    kafka_broker = config["DISTRIBUTION"]["kafka_broker"]
    username_writer = config["DISTRIBUTION"]["username_writer"]
    password_writer = config["DISTRIBUTION"]["password_writer"]

    year, month, day = args.night[0:4], args.night[4:6], args.night[6:8]
    basepath = os.path.join(
        mm_data_path, "online", "year={}/month={}/day={}".format(year, month, day)
    )
    checkpointpath_mm = os.path.join(mm_data_path, "mm_distribute_checkpoint")

    logger = get_fink_logger()
    wait = 5
    while True:
        try:
            logger.info("successfully connect to the MM database")
            # force the mangrove columns to have the struct type
            static_df = spark.read.parquet(basepath)

            path = basepath
            df_grb_stream = (
                spark.readStream.format("parquet")
                .schema(static_df.schema)
                .option("basePath", basepath)
                .option("path", path)
                .option("latestFirst", True)
                .load()
            )
            break

        except Exception:
            logger.info("Exception occured: wait: {}".format(wait), exc_info=1)
            time.sleep(wait)
            wait *= 1.2 if wait < 60 else 1
            continue

    df_grb_stream = (
        df_grb_stream.drop("brokerEndProcessTimestamp")
        .drop("brokerStartProcessTimestamp")
        .drop("brokerIngestTimestamp")
    )

    stream_distribute_list = grb_distribution_stream(
        df_grb_stream,
        static_df,
        checkpointpath_mm,
        args.tinterval,
        kafka_broker,
        username_writer,
        password_writer,
    )

    return stream_distribute_list
