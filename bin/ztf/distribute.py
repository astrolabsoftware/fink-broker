#!/usr/bin/env python
# Copyright 2019-2025 AstroLab Software
# Author: Abhishek Chauhan, Julien Peloton
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

"""Distribute the alerts to users

1. Use the Alert data that is stored in the Science TMP database (Parquet)
2. Apply user defined filters
3. Serialize into Avro
3. Publish to Kafka Topic(s)
"""

import argparse
import logging
import time

from fink_utils.spark import schema_converter
from fink_broker.common.parser import getargs
from fink_broker.common.spark_utils import (
    init_sparksession,
    connect_to_raw_database,
)
from fink_broker.common.distribution_utils import get_kafka_df
from fink_broker.common.logging_utils import init_logger
from fink_utils.spark.utils import concat_col
from fink_utils.spark.utils import apply_user_defined_filter


_LOG = logging.getLogger(__name__)

# User-defined topics
userfilters = [
    "fink_filters.ztf.filter_early_sn_candidates.filter.early_sn_candidates",
    "fink_filters.ztf.filter_sn_candidates.filter.sn_candidates",
    "fink_filters.ztf.filter_sso_ztf_candidates.filter.sso_ztf_candidates",
    "fink_filters.ztf.filter_sso_fink_candidates.filter.sso_fink_candidates",
    "fink_filters.ztf.filter_kn_candidates.filter.kn_candidates",
    "fink_filters.ztf.filter_early_kn_candidates.filter.early_kn_candidates",
    "fink_filters.ztf.filter_rate_based_kn_candidates.filter.rate_based_kn_candidates",
    "fink_filters.ztf.filter_microlensing_candidates.filter.microlensing_candidates",
    "fink_filters.ztf.filter_yso_candidates.filter.yso_candidates",
    "fink_filters.ztf.filter_simbad_grav_candidates.filter.simbad_grav_candidates",
    "fink_filters.ztf.filter_blazar.filter.blazar",
    "fink_filters.ztf.filter_yso_spicy_candidates.filter.yso_spicy_candidates",
    "fink_filters.ztf.filter_tns_match.filter.tns_match",
    "fink_filters.ztf.filter_magnetic_cvs.filter.magnetic_cvs",
    "fink_filters.ztf.filter_new_hostless.filter.new_hostless",
    "fink_filters.ztf.filter_new_hostless.filter.intra_night_hostless",
    "fink_filters.ztf.filter_new_hostless.filter.inter_night_hostless",
]


def push_to_kafka(df_in, topicname, cnames, checkpointpath_kafka, tinterval, kafka_cfg):
    """Push data to a Kafka custer

    Parameters
    ----------
    df_in: Spark DataFrame
        Alert DataFrame
    topicname: str
        Name of the Kafka topic to create
    cnames: list of str
        List of columns to transfer in the stream
    checkpointpath_kafka: str
        Path on HDFS/S3 for the checkpoints
    tinterval: int
        Interval in seconds between two micro-batches
    kafka_cfg: dict
        Dictionnary with Kafka parameters

    Returns
    -------
    out: Streaming DataFrame
    """
    df_in = df_in.selectExpr(cnames)

    # get schema from the streaming dataframe to
    # avoid non-nullable bug #852
    schema = schema_converter.to_avro(df_in.schema)

    df_kafka = get_kafka_df(df_in, key=schema, elasticc=False)

    disquery = (
        df_kafka.writeStream.format("kafka")
        .options(**kafka_cfg)
        .option("topic", topicname)
        .option("checkpointLocation", checkpointpath_kafka + "/" + topicname)
        .trigger(processingTime="{} seconds".format(tinterval))
        .start()
    )

    return disquery


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    logger = init_logger(args.log_level)

    logger.debug("Initialise Spark session")
    spark = init_sparksession(
        name="distribute_{}_{}".format(args.producer, args.night),
        shuffle_partitions=10,
        log_level=args.spark_log_level,
    )

    # data path
    scitmpdatapath = args.online_data_prefix + "/science/{}".format(args.night)
    checkpointpath_kafka = args.online_data_prefix + "/kafka_checkpoint/{}".format(
        args.night
    )

    logger.debug("Connect to the TMP science database")
    df = connect_to_raw_database(scitmpdatapath, scitmpdatapath, latestfirst=False)

    logger.debug("Cast fields to ease the distribution")
    cnames = df.columns

    if "brokerEndProcessTimestamp" in cnames:
        cnames[cnames.index("brokerEndProcessTimestamp")] = (
            "cast(brokerEndProcessTimestamp as string) as brokerEndProcessTimestamp"
        )
        cnames[cnames.index("brokerStartProcessTimestamp")] = (
            "cast(brokerStartProcessTimestamp as string) as brokerStartProcessTimestamp"
        )
        cnames[cnames.index("brokerIngestTimestamp")] = (
            "cast(brokerIngestTimestamp as string) as brokerIngestTimestamp"
        )

    cnames[cnames.index("cutoutScience")] = "struct(cutoutScience.*) as cutoutScience"
    cnames[cnames.index("cutoutTemplate")] = (
        "struct(cutoutTemplate.*) as cutoutTemplate"
    )
    cnames[cnames.index("cutoutDifference")] = (
        "struct(cutoutDifference.*) as cutoutDifference"
    )
    cnames[cnames.index("prv_candidates")] = (
        "explode(array(prv_candidates)) as prv_candidates"
    )
    cnames[cnames.index("candidate")] = "struct(candidate.*) as candidate"

    if not args.noscience:
        # This column is added by the science pipeline
        cnames[cnames.index("lc_features_g")] = (
            "struct(lc_features_g.*) as lc_features_g"
        )
        cnames[cnames.index("lc_features_r")] = (
            "struct(lc_features_r.*) as lc_features_r"
        )

    logger.debug("Retrieve time-series information")
    to_expand = [
        "jd",
        "fid",
        "magpsf",
        "sigmapsf",
        "magnr",
        "sigmagnr",
        "magzpsci",
        "isdiffpos",
        "diffmaglim",
    ]

    logger.debug("Append temp columns with historical + current measurements")
    prefix = "c"
    for colname in to_expand:
        df = concat_col(df, colname, prefix=prefix)

    # quick fix for https://github.com/astrolabsoftware/fink-broker/issues/457
    for colname in to_expand:
        df = df.withColumnRenamed("c" + colname, "c" + colname + "c")

    df = df.withColumn("cstampDatac", df["cutoutScience.stampData"])

    kafka_cfg = {
        "kafka.bootstrap.servers": args.distribution_servers,
    }

    if args.kafka_security_protocol == "SASL_PLAINTEXT":
        # CI - k8s
        kafka_cfg.setdefault("kafka.security.protocol", "SASL_PLAINTEXT")
        kafka_cfg.setdefault("kafka.sasl.mechanism", "SCRAM-SHA-512")
    elif args.kafka_security_protocol == "PLAINTEXT":
        # CI - sentinel
        kafka_cfg.setdefault("kafka.security.protocol", "PLAINTEXT")
    elif args.kafka_security_protocol == "VD":
        # VD
        kafka_cfg.setdefault("kafka.sasl.username", args.kafka_sasl_username)
        kafka_cfg.setdefault("kafka.sasl.password", args.kafka_sasl_password)
        kafka_cfg.setdefault("kafka.buffer.memory", args.kafka_buffer_memory)
        kafka_cfg.setdefault(
            "kafka.delivery.timeout.ms", args.kafka_delivery_timeout_ms
        )
    else:
        msg = " Kafka producer security protocol {} is not known".format(
            args.kafka_security_protocol
        )
        logger.warn(msg)
        spark.stop()

    for userfilter in userfilters:
        if args.noscience:
            logger.debug(
                "Do not apply user-defined filter %s in no-science mode", userfilter
            )
            df_tmp = df
        else:
            logger.debug("Apply user-defined filter %s", userfilter)
            df_tmp = apply_user_defined_filter(df, userfilter, _LOG)

        # The topic name is the filter name
        topicname = args.substream_prefix + userfilter.split(".")[-1] + "_ztf"

        # FIXME: shouldn't we collect in a list the disquery?
        disquery = push_to_kafka(
            df_tmp,
            topicname,
            cnames,
            checkpointpath_kafka,
            args.tinterval,
            kafka_cfg,
        )

    # Special filter to count alerts
    topicname = "fink_ztf_{}".format(args.night)
    disquery = push_to_kafka(
        df,
        topicname,
        ["objectId"],
        checkpointpath_kafka,
        args.tinterval,
        kafka_cfg,
    )

    if args.noscience:
        logger.info("Do not perform multi-messenger operations")
        time_spent_in_wait, stream_distrib_list = 0, None
    else:
        logger.debug("Perform multi-messenger operations")
        from fink_broker.ztf.mm_utils import distribute_launch_fink_mm

        time_spent_in_wait, stream_distrib_list = distribute_launch_fink_mm(spark, args)

    if args.exit_after is not None:
        remaining_time = args.exit_after - time_spent_in_wait
        remaining_time = remaining_time if remaining_time > 0 else 0
        logger.debug("Keep the Streaming for %s seconds", remaining_time)
        time.sleep(remaining_time)
        disquery.stop()
        if stream_distrib_list:
            for stream in stream_distrib_list:
                stream.stop()
        logger.info("Exiting the distribute service normally...")
    else:
        logger.debug("Wait for the end of queries")
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
