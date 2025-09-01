#!/usr/bin/env python
# Copyright 2019-2025
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
"""Kafka consumer to listen LSST stream"""

import os
import sys
import time
import logging
import numpy as np
from datetime import datetime

from fink_broker.rubin.decoding_utils import write_alert, return_offsets

from pyarrow.fs import HadoopFileSystem

import argparse

from multiprocessing import Process, Queue

from confluent_kafka import DeserializingConsumer
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

_LOG = logging.getLogger(__name__)


def run(q, kafka_config, config):
    """Single consumer poll

    Notes
    -----
    The consumer stops polling when the time crosses `config["stop_polling_at"]`

    Parameters
    ----------
    q: mutiprocessing.Queue
        Queue to store results
    kafka_config: dict
        Dictionary with Kafka parameters
    config: dict
        Dictionary with processing parameters

    Returns
    -------
    out: float
        Total time to process alerts
    """
    _LOG.debug("PID: {}".format(os.getpid()))
    if config["groupid"] is None:
        if not isinstance(config["seed_out"], int):
            # different group.id
            np.random.seed(None)
            rnd = np.random.randint(1e6)
        else:
            np.random.seed(config["seed_out"])
            rnd = np.random.randint(1e6)
        kafka_config["group.id"] += str(rnd)
    elif isinstance(config["groupid"], str):
        kafka_config["group.id"] += config["groupid"]
    else:
        raise TypeError("groupid must be None or string")
    _LOG.debug("group.id: {}".format(kafka_config["group.id"]))

    rng = np.random.default_rng(os.getpid())

    c = DeserializingConsumer(kafka_config)
    c.subscribe([config["topic"]])
    if config["hdfs_namenode"] != "":
        fs = HadoopFileSystem(
            config["hdfs_namenode"],
            config["hdfs_port"],
            user=config["hdfs_username"],
            replication=2,
        )
    else:
        # For local tests
        fs = None

    output_folder = os.path.join(config["online_data_prefix"], "raw", args.night)

    stop_polling_at_ = datetime.fromisoformat(config["stop_polling_at"])
    try:
        t0 = time.time()
        count = 0
        started = False
        msgs = []
        while True:
            message = c.poll(kafka_config["session.timeout.ms"] / 3000)
            if message is None:
                _LOG.debug("poll timeout")

                # dump on disk remaining alerts in the cache
                if len(msgs) > 0:
                    _LOG.info("Dump on disk remaining {} alerts...".format(len(msgs)))
                    write_alert(
                        msgs,
                        config["table_schema_path"],
                        config.get("avro_schema"),
                        fs,
                        rng.integers(0, 1e7),
                        where=output_folder,
                    )

                    msgs = []

                # check the date, and exit if need be
                if datetime.now() > stop_polling_at_:
                    _LOG.info("Finished polling...")
                    break

            elif message.error():
                _LOG.error("Error: {}".format(message.error()))
            else:
                if started is False:
                    _LOG.info("Start polling at: {}".format(datetime.now()))
                    t0 = time.time()
                    started = True
                deserialized = message.value()
                msgs.append(deserialized)
                count += 1

            if (count % config["hdfs_batch_size"] == 0) and (len(msgs) > 0):
                # Dump on disk
                write_alert(
                    msgs,
                    config["table_schema_path"],
                    config.get("avro_schema"),
                    fs,
                    rng.integers(0, 1e7),
                    where=output_folder,
                )

                if count >= config["max_alerts_per_consumer"]:
                    break

                # re-initialise containers
                msgs = []

        _LOG.debug("I am done at: {}".format(datetime.now()))
        t1 = time.time()
        _LOG.debug(
            "count: {} in {:.2f} seconds ({:2f} alerts/second)".format(
                count, t1 - t0, count / (t1 - t0)
            )
        )
    except KeyboardInterrupt:
        pass
    c.close()

    q.put(t1 - t0)
    return t1 - t0


if __name__ == "__main__":
    """Alert consumer for the LSST stream."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-lsst_kafka_server",
        type=str,
        default="",
        help="Kafka URL for LSST alerts",
    )
    parser.add_argument(
        "-lsst_schema_server",
        type=str,
        default="https://usdf-alert-schemas-dev.slac.stanford.edu",
        help="Kafka URL for schema",
    )
    parser.add_argument(
        "-lsst_kafka_username",
        type=str,
        default="",
        help="Username for Kafka",
    )
    parser.add_argument(
        "-lsst_kafka_password",
        type=str,
        default="",
        help="Password for Kafka",
    )
    parser.add_argument(
        "-hdfs_namenode",
        type=str,
        default="ccmaster1",
        help="HDFS namenode",
    )
    parser.add_argument(
        "-hdfs_port",
        type=int,
        default=8020,
        help="HDFS namenode port",
    )
    parser.add_argument(
        "-hdfs_username",
        type=str,
        default="fink",
        help="HDFS username",
    )
    parser.add_argument(
        "-nconsumers",
        type=int,
        default=1,
        help="Number of consumers to use in parallel. Default is 1.",
    )
    parser.add_argument(
        "-groupid",
        type=str,
        default=None,
        help="If specified, force the group.id",
    )
    parser.add_argument(
        "-max_alerts_per_consumer",
        type=int,
        default=1000,
        help="Maximum number of alerts to poll per consumer.",
    )
    parser.add_argument(
        "-hdfs_batch_size",
        type=int,
        default=100,
        help="Number of alerts to write per file.",
    )
    parser.add_argument(
        "-table_schema_path",
        type=str,
        default="rubin_parquet_schema",
        help="Local folder containing the alert schemas in the pyarrow format. Must contains latest_schema.txt",
    )
    parser.add_argument(
        "-avro_schema",
        type=str,
        default=None,
        help="PPath to an avro file used to simulate streams. Only used in the Continuous integration. Default is None.",
    )
    parser.add_argument(
        "-online_data_prefix",
        type=str,
        default="online",
        help="Folder name to store alerts. Can be local or HDFS.",
    )
    parser.add_argument(
        "-stop_polling_at",
        type=str,
        default="2055-11-03 00:00:00",
        help="Date YYYY-MM-DD [hh:mm:ss] from when the consumers should stop polling. The stop will happen after the next timeout.",
    )
    parser.add_argument(
        "-log",
        type=str,
        default="INFO",
        help="Logging level: DEBUG, INFO (default), WARN, ERROR.",
    )
    parser.add_argument(
        "-topic",
        type=str,
        default="alerts-simulated",
        help="Kafka topic name",
    )
    parser.add_argument(
        "-night",
        type=str,
        default="20250701",
        help="Observing night, in format YYYYMMDD",
    )
    parser.add_argument(
        "--different_groupid",
        action="store_true",
        help="If specified, all consumers will belong to different group.id. Only for testing.",
    )
    parser.add_argument(
        "--check_offsets",
        action="store_true",
        help="If specified, check the size of the topic.",
    )
    args = parser.parse_args(None)

    if (args.lsst_kafka_username == "") or (args.lsst_kafka_password == ""):
        _LOG.error(
            "LSST_KAFKA_USERNAME and LSST_KAFKA_PASSWORD are empty and should be set"
        )

    sr_client = SchemaRegistryClient({"url": args.lsst_schema_server})
    deserializer = AvroDeserializer(sr_client)

    kafka_config = {
        # This is the URL to use to connect to the Kafka cluster.
        "bootstrap.servers": args.lsst_kafka_server,
        # The Consumer Group ID, as described above.
        "group.id": args.lsst_kafka_username + "_group_",
        "auto.offset.reset": "earliest",
        "fetch.min.bytes": 10 * 1024 * 1024,
        "max.poll.interval.ms": 300000,
        "session.timeout.ms": 10000,
    }

    config = {
        "topic": args.topic,
        "online_data_prefix": args.online_data_prefix,
        "hdfs_namenode": args.hdfs_namenode,
        "hdfs_port": args.hdfs_port,
        "hdfs_username": args.hdfs_username,
        "max_alerts_per_consumer": args.max_alerts_per_consumer,
        "hdfs_batch_size": args.hdfs_batch_size,
        "table_schema_path": args.table_schema_path,
        "groupid": args.groupid,
        "stop_polling_at": args.stop_polling_at,
        "night": args.night,
        "nconsumers": args.nconsumers,
    }

    if args.lsst_kafka_username != "ci":
        kafka_config.update({
            # These next two properties tell the Kafka client about the specific
            # authentication and authorization protocols that should be used when
            # connecting.
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanisms": "SCRAM-SHA-512",
            # The sasl.username and sasl.password are passed through over
            # SCRAM-SHA-512 auth to connect to the cluster. The username is not
            # sensitive, but the password is (of course) a secret value which
            # should never be committed to source code.
            "sasl.username": args.lsst_kafka_username,
            "sasl.password": args.lsst_kafka_password,
            # Finally, we pass in the deserializer that we created above,
            # configuring the consumer so that it automatically does all the Schema
            # Registry and Avro deserialization work.
            "value.deserializer": deserializer,
        })
    else:
        config.update({"avro_schema": args.avro_schema})

    # Set logging level
    numeric_level = getattr(logging, args.log.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError("Invalid log level: {loglevel}")
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s %(levelname)s:%(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p",
    )

    # check topic exists
    kadmin = AdminClient({
        k: v for k, v in kafka_config.items() if k != "value.deserializer"
    })
    available_topics = kadmin.list_topics().topics
    while args.topic not in available_topics:
        _LOG.warning(
            "{} is not in the list of available topics: {}. Sleeping 1 minute...".format(
                args.topic, available_topics
            )
        )
        time.sleep(60)
    kadmin.close()

    if args.check_offsets:
        if config["groupid"] is not None:
            kafka_config["group.id"] += config["groupid"]
        _LOG.info("group.id: {}".format(kafka_config["group.id"]))
        c = DeserializingConsumer(kafka_config)
        return_offsets(
            consumer=c,
            topic=config["topic"],
            hide_empty_partition=False,
            verbose=True,
        )
        sys.exit()

    _LOG.info(
        "Number of consumers: {} with {} alerts each".format(
            config["nconsumers"], config["max_alerts_per_consumer"]
        )
    )

    # Start the experiment
    t_start = time.time()

    workers = []
    q = Queue()

    config["seed_out"] = None if args.different_groupid else np.random.randint(1e6)
    for _ in range(config["nconsumers"]):
        w = Process(
            target=run,
            args=(
                q,
                kafka_config,
                config,
            ),
        )
        w.start()
        workers.append(w)

    for w in workers:
        w.join()

    # Collect times
    times = []
    while not q.empty():
        if q.empty():
            break
        times.append(q.get())

    times = np.array(times)
    _LOG.info(
        "Time to poll per consumer: {:.2f} +/- {:.2f} seconds".format(
            np.mean(times), np.std(times)
        )
    )
    _LOG.info(
        "Throughput per consumer: {:.2f} +/- {:.2f} alerts/s".format(
            np.mean(config["max_alerts_per_consumer"] / times),
            np.std(config["max_alerts_per_consumer"] / times),
        )
    )
    _LOG.info("Total time to solution: {:.2f} seconds".format(time.time() - t_start))
