#!/usr/bin/env python
# Copyright 2019-2024 AstroLab Software
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
import time
import os

from fink_utils.spark import schema_converter
from fink_broker.parser import getargs
from fink_broker.mm_utils import mm2distribute
from fink_broker.spark_utils import (
    init_sparksession,
    connect_to_raw_database,
    path_exist,
)
from fink_broker.distribution_utils import get_kafka_df
from fink_broker.logging_utils import get_fink_logger, inspect_application

from fink_utils.spark.utils import concat_col

from fink_utils.spark.utils import apply_user_defined_filter

from fink_mm.init import get_config

# User-defined topics
userfilters = [
    "fink_filters.filter_early_sn_candidates.filter.early_sn_candidates",
    "fink_filters.filter_sn_candidates.filter.sn_candidates",
    "fink_filters.filter_sso_ztf_candidates.filter.sso_ztf_candidates",
    "fink_filters.filter_sso_fink_candidates.filter.sso_fink_candidates",
    "fink_filters.filter_kn_candidates.filter.kn_candidates",
    "fink_filters.filter_early_kn_candidates.filter.early_kn_candidates",
    "fink_filters.filter_rate_based_kn_candidates.filter.rate_based_kn_candidates",
    "fink_filters.filter_microlensing_candidates.filter.microlensing_candidates",
    "fink_filters.filter_yso_candidates.filter.yso_candidates",
    "fink_filters.filter_simbad_grav_candidates.filter.simbad_grav_candidates",
    "fink_filters.filter_blazar.filter.blazar",
    "fink_filters.filter_yso_spicy_candidates.filter.yso_spicy_candidates",
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(
        name="distribute_{}_{}".format(args.producer, args.night), shuffle_partitions=2
    )

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    # data path
    scitmpdatapath = args.online_data_prefix + "/science/{}".format(args.night)
    checkpointpath_kafka = args.online_data_prefix + "/kafka_checkpoint/{}".format(
        args.night
    )

    # Connect to the TMP science database
    df = connect_to_raw_database(scitmpdatapath, scitmpdatapath, latestfirst=False)

    # Cast fields to ease the distribution
    cnames = df.columns
    # cnames[cnames.index('timestamp')] = 'cast(timestamp as string) as timestamp'

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

    # Extract schema
    df_schema = spark.read.format("parquet").load(scitmpdatapath)
    df_schema = df_schema.selectExpr(cnames)

    schema = schema_converter.to_avro(df_schema.coalesce(1).limit(1).schema)

    # Retrieve time-series information
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

    # Append temp columns with historical + current measurements
    prefix = "c"
    for colname in to_expand:
        df = concat_col(df, colname, prefix=prefix)

    # quick fix for https://github.com/astrolabsoftware/fink-broker/issues/457
    for colname in to_expand:
        df = df.withColumnRenamed("c" + colname, "c" + colname + "c")

    df = df.withColumn("cstampDatac", df["cutoutScience.stampData"])

    broker_list = args.distribution_servers
    username = args.kafka_sasl_username
    password = args.kafka_sasl_password
    kafka_buf_mem = args.kafka_buffer_memory
    kafka_timeout_ms = args.kafka_delivery_timeout_ms
    for userfilter in userfilters:
        # The topic name is the filter name
        topicname = args.substream_prefix + userfilter.split(".")[-1] + "_ztf"

        # Apply user-defined filter
        if args.noscience:
            df_tmp = df
        else:
            df_tmp = apply_user_defined_filter(df, userfilter, logger)

        # Wrap alert data
        df_tmp = df_tmp.selectExpr(cnames)

        # Get the DataFrame for publishing to Kafka (avro serialized)
        df_kafka = get_kafka_df(df_tmp, key=schema, elasticc=False)

        # Ensure that the topic(s) exist on the Kafka Server)
        disquery = (
            df_kafka.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", broker_list)
            .option("kafka.security.protocol", "SASL_PLAINTEXT")
            .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
            .option("kafka.sasl.username", username)
            .option("kafka.sasl.password", password)
            .option("kafka.buffer.memory", kafka_buf_mem)
            .option("kafka.delivery.timeout.ms", kafka_timeout_ms)
            .option("kafka.auto.create.topics.enable", True)
            .option("topic", topicname)
            .option("checkpointLocation", checkpointpath_kafka + "/" + topicname)
            .trigger(processingTime="{} seconds".format(args.tinterval))
            .start()
        )

    config_path = args.mmconfigpath
    count = 0
    stream_distrib_list = None

    # Keep the Streaming running until something or someone ends it!
    if args.exit_after is not None:
        if config_path != "no-config":
            config = get_config({"--config": config_path})

            while count < args.exit_after:
                mm_path_output = config["PATH"]["online_grb_data_prefix"]
                mmtmpdatapath = os.path.join(mm_path_output, "online")

                # if there is gcn and ztf data
                if path_exist(mmtmpdatapath):
                    t_before = time.time()
                    logger.info("starting mm2distribute ...")
                    stream_distrib_list = mm2distribute(spark, config, args)
                    count += time.time() - t_before
                    break

                count += 1
                time.sleep(1.0)

        remaining_time = args.exit_after - count
        remaining_time = remaining_time if remaining_time > 0 else 0
        time.sleep(remaining_time)
        disquery.stop()
        if stream_distrib_list is not None:
            for stream in stream_distrib_list:
                stream.stop()
        logger.info("Exiting the distribute service normally...")
    else:
        if config_path != "no-config":
            config = get_config({"--config": config_path})
            stream_distrib_list = mm2distribute(spark, config, args)
        # Wait for the end of queries
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
