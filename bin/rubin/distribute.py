#!/usr/bin/env python
# Copyright 2019-2026 AstroLab Software
# Author: Julien Peloton, Massinissa MACHTER
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
"""Distribute the alerts to users and store data to HBase tables

1. Use the Alert data that is stored in the Science TMP data lake (Parquet)
2. Apply user defined filters
3. Store data to HBase tables
4. Serialize into Avro
5. Publish to Kafka Topics
"""

from pyspark.sql.types import BooleanType
import pyspark.sql.functions as F

import pkgutil
import argparse
import logging
import time

from fink_broker.common.distribution_utils import push_to_kafka, FakeQuery
from fink_broker.common.logging_utils import init_logger
from fink_broker.common.parser import getargs
from fink_broker.common.spark_utils import (
    init_sparksession,
    connect_to_raw_database,
)
from fink_broker.rubin.spark_utils import apply_kafka_serialisation
from fink_broker.rubin.spark_utils import get_schema_from_parquet

from fink_utils.spark.utils import (
    expand_function_from_string,
    FinkUDF,
)

import fink_filters.rubin.livestream as ffrl
from fink_broker.rubin.hbase_utils import ingest_section


_LOG = logging.getLogger(__name__)

# User-defined topics
userfilters = [
    "{}.{}.filter.{}".format(ffrl.__package__, mod, mod.split("filter_")[1])
    for _, mod, _ in pkgutil.iter_modules(ffrl.__path__)
]


def main():
    """TBD"""
    # FIXME: should we have CLI args to do streaming and/or ingestion?
    # FIXME: such as fink start distribute --ingest --distribute
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
    checkpointpath_hbase = args.online_data_prefix + "/hbase_checkpoint/{}".format(
        args.night
    )

    logger.debug("Connect to the TMP science database")
    df = connect_to_raw_database(scitmpdatapath, scitmpdatapath, latestfirst=False)

    logger.debug("Cast fields to ease the distribution")
    cnames = df.columns
    cnames = apply_kafka_serialisation(cnames)

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

    # Apply or not the filtering
    if args.noscience:
        topics = []

        for userfilter in userfilters:
            logger.debug(
                "Do not apply user-defined filter %s in no-science mode", userfilter
            )
            topicname = args.substream_prefix + userfilter.split(".")[-1] + "_lsst"
            topics.append(F.lit(topicname))

        df_with_topics = df.withColumn("topics", F.array(*topics))

        df_filtered = df_with_topics.withColumn("topic", F.explode("topics"))
    else:
        topic_exprs = []
        for userfilter in userfilters:
            logger.debug("Apply user-defined filter %s", userfilter)

            # build filter function expr dynamically
            filter_func, colnames = expand_function_from_string(df, userfilter)
            tag = userfilter.split(".")[-1]
            fink_filter = FinkUDF(
                filter_func,
                BooleanType(),
                tag,
            )
            expr = fink_filter.for_spark(*colnames)

            topicname = args.substream_prefix + tag + "_lsst"

            topic_exprs.append(F.when(expr, F.lit(topicname)))

        # array_compact for delete NULL values in array
        df_with_topics = df.withColumn("topics", F.array_compact(F.array(*topic_exprs)))

        df_filtered = df_with_topics.withColumn("topic", F.explode("topics"))

    # All filters distributed to multiple kafka topics with 1 writeStream
    if not args.no_kafka_ingest:
        kafka_query = push_to_kafka(
            df_filtered,
            cnames,
            checkpointpath_kafka + "/{}filters_lsst".format(args.substream_prefix),
            args.tinterval,
            kafka_cfg,
            npart=None,
        )
    else:
        logger.warning("Skipping Kafka ingestion")
        kafka_query = FakeQuery()

    # Hbase ingestion (Hbase not support dynamic routing via column "table" + OneWriteStream)
    if not args.no_hbase_ingest:
        # HBase ingestion
        major_version, minor_version = get_schema_from_parquet(scitmpdatapath)
        # Key is time_oid to perform date range search
        cols_row_key_name = ["midpointMjdTai", "diaObjectId"]
        row_key_name = "_".join(cols_row_key_name)

        hbase_queries = []

        # Adding `df_filtred.persiste()` here might be preferable, right?
        for userfilter in userfilters:
            tag = userfilter.split(".")[-1]
            table_name = "{}.tag_{}".format(args.science_db_name, tag)
            topicname = args.substream_prefix + tag + "_lsst"

            df_filtered_tag = df_filtered.filter(F.col("topic") == topicname)

            hbase_query = ingest_section(
                df_filtered_tag,
                major_version,
                minor_version,
                row_key_name,
                table_name=table_name,
                catfolder=args.science_db_catalogs,
                cols_row_key_name=cols_row_key_name,
                streaming=True,
                checkpoint_path=checkpointpath_hbase + "/" + tag,
            )
            hbase_queries.append(hbase_query)
    else:
        logger.warning("Skipping HBase ingestion")
        hbase_queries = [FakeQuery()]

    if args.exit_after is not None:
        logger.debug("Keep the Streaming running until something or someone ends it!")
        remaining_time = args.exit_after
        remaining_time = remaining_time if remaining_time > 0 else 0
        time.sleep(remaining_time)
        kafka_query.stop()
        for hbase_query in hbase_queries:
            hbase_query.stop()
        logger.info("Exiting the distribute service normally...")
    else:
        logger.debug("Wait for the end of queries")
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
