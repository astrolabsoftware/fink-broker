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

from utils import write_alert, return_offsets

from pyarrow.fs import HadoopFileSystem

import argparse

from multiprocessing import Process, Queue

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

_LOG = logging.getLogger(__name__)

# Load input parameters from environment variables:

# $KAFKA_USERNAME should be the username of the community broker.
# $KAFKA_PASSWORD should be the password; this doesn't have a default and must
USERNAME = os.environ.get("LSST_KAFKA_USERNAME", "")
PASSWORD = os.environ.get("LSST_KAFKA_PASSWORD", "")

if (USERNAME == "") or (PASSWORD == ""):
    _LOG.warning("USERNAME and PASSWORD are empty and should be set")

# These are the URLs for the integration environment of the alert stream on the
# US data facility (aka the "USDF" environment).
SERVER = "usdf-alert-stream-dev.lsst.cloud:9094"
SCHEMA_REG_URL = "https://usdf-alert-schemas-dev.slac.stanford.edu"
sr_client = SchemaRegistryClient({"url": SCHEMA_REG_URL})
deserializer = AvroDeserializer(sr_client)

config = {
    # This is the URL to use to connect to the Kafka cluster.
    "bootstrap.servers": SERVER,
    # These next two properties tell the Kafka client about the specific
    # authentication and authorization protocols that should be used when
    # connecting.
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanisms": "SCRAM-SHA-512",
    # The sasl.username and sasl.password are passed through over
    # SCRAM-SHA-512 auth to connect to the cluster. The username is not
    # sensitive, but the password is (of course) a secret value which
    # should never be committed to source code.
    "sasl.username": USERNAME,
    "sasl.password": PASSWORD,
    # The Consumer Group ID, as described above.
    "group.id": USERNAME + "_group_",
    # Finally, we pass in the deserializer that we created above,
    # configuring the consumer so that it automatically does all the Schema
    # Registry and Avro deserialization work.
    "value.deserializer": deserializer,
    "auto.offset.reset": "earliest",
    "fetch.min.bytes": 10 * 1024 * 1024,
    "max.poll.interval.ms": 300000,
    "session.timeout.ms": 10000,
}


def run(
    q,
    max_alerts_per_consumer,
    hdfs_batch_size,
    table_schema_path,
    groupid,
    hdfs_folder,
    stop_polling_at,
    seed_out,
):
    """Single consumer poll"""
    _LOG.debug("PID: {}".format(os.getpid()))
    if groupid is None:
        if not isinstance(seed_out, int):
            # different group.id
            np.random.seed(None)
            rnd = np.random.randint(1e6)
        else:
            np.random.seed(seed_out)
            rnd = np.random.randint(1e6)
        config["group.id"] += str(rnd)
    elif isinstance(groupid, str):
        config["group.id"] += groupid
    else:
        raise TypeError("groupid must be None or string")
    _LOG.debug("group.id: {}".format(config["group.id"]))

    rng = np.random.default_rng(os.getpid())

    c = DeserializingConsumer(config)
    c.subscribe(["alerts-simulated"])
    fs = HadoopFileSystem("ccmaster1", 8020, user="fink", replication=2)

    stop_polling_at_ = datetime.fromisoformat(stop_polling_at)
    try:
        t0 = time.time()
        count = 0
        started = False
        msgs = []
        while True:
            message = c.poll(30.0)
            if message is None:
                _LOG.info("poll timeout")

                # dump on disk remaining alerts in the cache
                if len(msgs) > 0:
                    _LOG.info("Dump on disk remaining {} alerts...".format(len(msgs)))
                    write_alert(
                        msgs,
                        table_schema_path,
                        fs,
                        rng.integers(0, 1e7),
                        where=hdfs_folder,
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
                    _LOG.debug("Start polling at: {}".format(datetime.now()))
                    t0 = time.time()
                    started = True
                deserialized = message.value()
                msgs.append(deserialized)
                count += 1

            if (count % hdfs_batch_size == 0) and (len(msgs) > 0):
                # Dump on disk
                write_alert(
                    msgs,
                    table_schema_path,
                    fs,
                    rng.integers(0, 1e7),
                    where=hdfs_folder,
                )

                if count >= max_alerts_per_consumer:
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
    """ """
    parser = argparse.ArgumentParser(description=__doc__)
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
        default="rubin_parquet_schema/lsst.v7_1.parquet",
        help="HDFS path to a parquet file containing the alert schema in the pyarrow format",
    )
    parser.add_argument(
        "-hdfs_folder",
        type=str,
        default="test",
        help="Folder name on HDFS to store alerts. Typically basename/YYYYMMDD",
    )
    parser.add_argument(
        "-stop_polling_at",
        type=str,
        default="2055-11-03 00:00:00",
        help="Date YYYY-MM-DD [hh:mm:ss] from when the consumers should stop polling. The stop will happen after the next timeout.",
    )
    parser.add_argument(
        "--log",
        type=str,
        default="INFO",
        help="Logging level: DEBUG, INFO (default), WARN, ERROR.",
    )
    parser.add_argument(
        "--different_groupid",
        action="store_true",
        help="If specified, all consumers will belong to different group.id.",
    )
    parser.add_argument(
        "--check_offsets",
        action="store_true",
        help="If specified, check the size of the topic.",
    )
    args = parser.parse_args(None)

    # Set logging level
    numeric_level = getattr(logging, args.log.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError("Invalid log level: {loglevel}")
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s %(levelname)s:%(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p",
    )

    if args.check_offsets:
        if args.groupid is not None:
            config["group.id"] += args.groupid
        _LOG.info("group.id: {}".format(config["group.id"]))
        c = DeserializingConsumer(config)
        return_offsets(
            consumer=c,
            topic="alerts-simulated",
            hide_empty_partition=False,
            verbose=True,
        )
        sys.exit()

    _LOG.info(
        "Number of consumers: {} with {} alerts each".format(
            args.nconsumers, args.max_alerts_per_consumer
        )
    )

    # Start the experiment
    t_start = time.time()

    workers = []
    q = Queue()

    seed_out = None if args.different_groupid else np.random.randint(1e6)
    for _ in range(args.nconsumers):
        w = Process(
            target=run,
            args=(
                q,
                args.max_alerts_per_consumer,
                args.hdfs_batch_size,
                args.table_schema_path,
                args.groupid,
                args.hdfs_folder,
                args.stop_polling_at,
                seed_out,
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
            np.mean(args.max_alerts_per_consumer / times),
            np.std(args.max_alerts_per_consumer / times),
        )
    )
    _LOG.info("Total time to solution: {:.2f} seconds".format(time.time() - t_start))
