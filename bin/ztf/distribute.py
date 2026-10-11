#!/usr/bin/env python
# Copyright 2019-2025 AstroLab Software
# Author: Abhishek Chauhan, Julien Peloton, Massinissa MACHTER
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

import pyspark.sql.functions as F

import pkgutil
import argparse
import logging
import sys
import time

from fink_broker.common.parser import getargs
from fink_broker.common.night_utils import get_exit_deadline, seconds_until
from fink_broker.common.spark_utils import (
    init_sparksession,
    NoDataAvailableError,
    connect_to_raw_database,
    wait_for_filesystem,
    wait_for_kafka,
)
from fink_broker.common.distribution_utils import push_to_kafka
from fink_broker.common.logging_utils import init_logger
from fink_utils.spark.utils import concat_col
from fink_utils.spark.utils import expand_function_from_string
import fink_filters.ztf.livestream as ffzl


_LOG = logging.getLogger(__name__)

# User-defined topics
userfilters = [
    "{}.{}.filter.{}".format(ffzl.__package__, mod, mod.split("filter_")[1])
    for _, mod, _ in pkgutil.iter_modules(ffzl.__path__)
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    logger = init_logger(args.log_level)

    # Anchored on the day the job starts, so a restart aims at the same
    # instant instead of granting itself a fresh window.
    exit_deadline = get_exit_deadline(args.exit_at)

    # A deadline already in the past means the job cannot honour its window:
    # report it rather than run a full extra one.
    if exit_deadline is not None and seconds_until(exit_deadline) <= 0:
        logger.warning(
            "The exit_at deadline %s has already passed: alerts of night %s "
            "may not have been distributed",
            exit_deadline,
            args.night,
        )
        sys.exit(1)

    logger.debug("Initialise Spark session")
    spark = init_sparksession(
        name="distribute_{}_{}".format(args.producer, args.night),
        shuffle_partitions=10,
        log_level=args.spark_log_level,
    )

    # This job is deployed alongside the services it depends on, so they may
    # not be up yet. Wait for them instead of failing on the first access, but
    # never past the stop instant: waiting into the next tick would hold the
    # Forbid slot for a run whose window is already over.
    wait_for_kafka(args.distribution_servers, deadline=exit_deadline)
    wait_for_filesystem(args.online_data_prefix, deadline=exit_deadline)

    # data path
    scitmpdatapath = args.online_data_prefix + "/science/{}".format(args.night)
    checkpointpath_kafka = args.online_data_prefix + "/kafka_checkpoint/{}".format(
        args.night
    )

    logger.debug("Connect to the TMP science database")
    try:
        df = connect_to_raw_database(
            scitmpdatapath, scitmpdatapath, latestfirst=False, deadline=exit_deadline
        )
    except NoDataAvailableError as e:
        # The telescope does not observe every night. Exit successfully so the
        # scheduler frees the slot instead of retrying a job with no input.
        logger.info(
            "No alert processed for night %s, nothing to do (%s)", args.night, e
        )
        return

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

    if not args.noscience:
        # For YSO SPICY
        df = df.withColumn("linear_fit_slope", F.col("lc_features_r.linear_fit_slope"))
    else:
        # fake big value for the slope
        df = df.withColumn("linear_fit_slope", F.lit(1.0))

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

    if args.noscience:
        topics = []
        for userfilter in userfilters:
            logger.debug(
                "Do not apply user-defined filter %s in no-science mode", userfilter
            )

            topicname = args.substream_prefix + userfilter.split(".")[-1] + "_ztf"
            topics.append(F.lit(topicname))
        df_with_topics = df.withColumn("topics", F.array(*topics))

        df_filtered = df_with_topics.withColumn("topic", F.explode("topics"))

    else:
        topic_exprs = []
        for userfilter in userfilters:
            logger.debug("Apply user-defined filter %s", userfilter)

            # build filter function dynamically
            filter_func, colnames = expand_function_from_string(df, userfilter)

            topicname = args.substream_prefix + userfilter.split(".")[-1] + "_ztf"

            topic_exprs.append(F.when(filter_func(*colnames), F.lit(topicname)))
        # array_compact for delete NULL in array
        df_with_topics = df.withColumn("topics", F.array_compact(F.array(*topic_exprs)))

        df_filtered = df_with_topics.withColumn("topic", F.explode("topics"))

    # push to kafka (df_filtred) with One writeStream, using the column topic (not .option("topic",...)
    disquery1 = push_to_kafka(
        df_filtered,
        cnames,
        checkpointpath_kafka + "/{}filters_ztf".format(args.substream_prefix),
        args.tinterval,
        kafka_cfg,
        npart=10,
    )

    # Special filter to count alerts
    topicname = "fink_ztf_{}".format(args.night)
    df = df.withColumn("topic", F.lit(topicname))
    disquery2 = push_to_kafka(
        df,
        ["objectId"],
        checkpointpath_kafka + "/" + topicname,
        args.tinterval,
        kafka_cfg,
    )

    if exit_deadline is not None:
        # An absolute deadline already accounts for whatever was spent waiting
        # for the upstream data, so nothing is subtracted here.
        logger.debug("Keep the Streaming until the exit_at deadline %s", exit_deadline)
        time.sleep(seconds_until(exit_deadline))
        disquery.stop()
        logger.info(
            "Reached the exit_at deadline %s, exiting normally...", exit_deadline
        )
    elif args.exit_after is not None:
        remaining_time = args.exit_after
        remaining_time = remaining_time if remaining_time > 0 else 0
        logger.debug("Keep the Streaming for %s seconds", remaining_time)
        time.sleep(remaining_time)
        disquery1.stop()
        disquery2.stop()
        logger.info("Exiting the distribute service normally...")
    else:
        logger.debug("Wait for the end of queries")
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
