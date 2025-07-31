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
import os
import requests
import argparse
import logging
import pyarrow.parquet as pq

from pyspark.sql import SparkSession

_LOG = logging.getLogger(__name__)


if __name__ == "__main__":
    """Fetch latest LSST alert schema"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-schema_url",
        type=str,
        default="https://usdf-alert-schemas-dev.slac.stanford.edu",
        help="URL for LSST schema",
    )
    parser.add_argument(
        "-table_schema_path",
        type=str,
        default="./rubin_parquet_schema",
        help="Local folder containing schemas in the pyarrow format",
    )
    args = parser.parse_args(None)

    # Check folder exists
    if not os.path.exists(args.table_schema_path):
        _LOG.error(
            "{} does not exist. Create it before!".format(args.table_schema_path)
        )
        raise FileNotFoundError(args.table_schema_path)

    response = requests.get(
        "{}/subjects/{}/versions/latest/schema".format(args.schema_url, "alert-packet")
    )
    response.raise_for_status()

    # check if schema exists
    version = response.json()["namespace"]

    # fs = HadoopFileSystem(args.hdfs_namenode, args.hdfs_port, user=args.hdfs_user, replication=2)
    filename = os.path.join(args.table_schema_path, "{}.parquet".format(version))

    exists = True
    try:
        table_schema = pq.read_schema(filename)
        _LOG.warning("Schema version {} already exists at {}".format(version, filename))
    except FileNotFoundError:
        _LOG.info("New schema detected: {}".format(version))
        exists = False

    if not exists:
        _LOG.info("Saving schema at {}...".format(args.table_schema_path))
        # save it
        schema = response.text

        spark = SparkSession.builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        # save an empty dataframe with that schema
        spark.read.format("avro").option(
            "avroSchema", schema
        ).load().toPandas().to_parquet(
            os.path.join(args.table_schema_path, "{}.parquet".format(version))
        )

        with open(os.path.join(args.table_schema_path, "latest_schema.log"), "w") as f:
            f.write("{}.parquet".format(version))
