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

import argparse
import configparser
import logging
import os
import time
from typing import Tuple, List

from fink_mm.ztf_join_gcn import ztf_join_gcn_stream, DataMode
from fink_mm.distribution.distribution import grb_distribution_stream

from astropy.time import Time
from datetime import timedelta

from fink_broker.spark_utils import connect_to_raw_database, path_exist
from fink_broker.logging_utils import init_logger

from pyspark.sql.streaming import StreamingQuery

_LOG = logging.getLogger(__name__)


def distribute_launch_fink_mm(spark, args: dict) -> Tuple[int, List]:
    """Manage multimessenger operations

    Parameters
    ----------
    spark: SparkSession
        Spark Session
    args: dict
        Arguments from Fink configuration file

    Returns
    -------
    time_spent_in_wait: int
        Time spent in waiting for GCN to come
        before launching the streaming query.
    stream_distrib_list: list of StreamingQuery
        List of Spark Streaming queries

    """
    if args.mmconfigpath != "no-config":
        from fink_mm.init import get_config
        from fink_broker.mm_utils import mm2distribute

        _LOG.info("Fink-MM configuration file: {args.mmconfigpath}")
        config = get_config({"--config": args.mmconfigpath})

        # Wait for GCN comming
        time_spent_in_wait = 0
        stream_distrib_list = []
        while time_spent_in_wait < args.exit_after:
            mm_path_output = config["PATH"]["online_grb_data_prefix"]
            mmtmpdatapath = os.path.join(mm_path_output, "online")

            # if there is gcn and ztf data
            if path_exist(mmtmpdatapath):
                t_before = time.time()
                _LOG.info("starting mm2distribute ...")
                stream_distrib_list = mm2distribute(spark, config, args)
                time_spent_in_wait += time.time() - t_before
                break

            time_spent_in_wait += 1
            time.sleep(1.0)
        if stream_distrib_list == []:
            _LOG.warning(
                f"{mmtmpdatapath} does not exist. mm2distribute could not start before the end of the job."
            )
        else:
            _LOG.info("Time spent in waiting for Fink-MM: {time_spent_in_wait} seconds")
        return time_spent_in_wait, stream_distrib_list

    _LOG.warning("No configuration found for fink-mm -- not applied")
    return 0, []

def raw2science_launch_fink_mm(args: dict, scitmpdatapath: str) -> Tuple[int, StreamingQuery]:
    """Manage multimessenger operations

    Parameters
    ----------
    args: dict
        Arguments from Fink configuration file
    scitmpdatapath: str
        Path to Fink alert data (science)

    Returns
    -------
    time_spent_in_wait: int
        Time spent in waiting for GCN to come
        before launching the streaming query.
    countquery_mm: StreamingQuery
        Spark Streaming query

    """
    if args.mmconfigpath != "no-config":
        from fink_mm.init import get_config
        from fink_broker.mm_utils import science2mm

        _LOG.info("Fink-MM configuration file: {args.mmconfigpath}")
        config = get_config({"--config": args.mmconfigpath})
        gcndatapath = config["PATH"]["online_gcn_data_prefix"]
        gcn_path = gcndatapath + "/year={}/month={}/day={}".format(
            args.night[0:4], args.night[4:6], args.night[6:8]
        )

        # Wait for GCN comming
        time_spent_in_wait = 0
        countquery_mm = None
        while time_spent_in_wait < args.exit_after:
            # if there is gcn and ztf data
            if path_exist(gcn_path) and path_exist(scitmpdatapath):
                # Start the GCN x ZTF cross-match stream
                t_before = time.time()
                _LOG.info("starting science2mm ...")
                countquery_mm = science2mm(args, config, gcn_path, scitmpdatapath)
                time_spent_in_wait += time.time() - t_before
                break
            else:
                # wait for comming GCN
                time_spent_in_wait += 1
                time.sleep(1)

        if countquery_mm is None:
            _LOG.warning("science2mm could not start before the end of the job.")
        else:
            _LOG.info("Time spent in waiting for Fink-MM: {time_spent_in_wait} seconds")
        return time_spent_in_wait, countquery_mm

    _LOG.warning("No configuration found for fink-mm -- not applied")
    return 0, None


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
    logger = init_logger(args.spark_log_level)
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

    logger = init_logger(args.spark_log_level)
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
