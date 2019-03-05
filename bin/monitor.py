#!/usr/bin/env python
# Copyright 2018 Julien Peloton
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
"""Monitor Kafka stream received by Spark
"""
from pyspark.sql import SparkSession

import argparse
import time

from fink_broker import monitoring

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        'servers', type=str,
        help='Hostname or IP and port of Kafka broker producing stream.')
    parser.add_argument(
        'topic', type=str,
        help='Name of Kafka topic stream to read from.')
    parser.add_argument(
        'finkwebpath', type=str,
        help='Folder to store UI data for display. See conf/fink.conf')
    args = parser.parse_args()

    # Grab the running Spark Session,
    # otherwise create it.
    spark = SparkSession \
        .builder \
        .appName("monitorStream") \
        .getOrCreate()

    # Create a streaming DF from the incoming stream from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args.servers) \
        .option("subscribe", args.topic) \
        .option("startingOffsets", "latest") \
        .load()

    # keep the size of shuffles small
    spark.conf.set("spark.sql.shuffle.partitions", "2")

    # Trigger the streaming computation,
    # by defining the sink (memory here) and starting it
    countQuery = df \
        .writeStream \
        .queryName("qraw")\
        .format("memory")\
        .outputMode("update") \
        .start()

    colnames = ["inputRowsPerSecond", "processedRowsPerSecond", "timestamp"]

    while True:
        try:
            monitoring.save_monitoring(args.finkwebpath, countQuery, colnames)
        except TypeError:
            print(
                """
                No Data to plot - Retyring....
                server: {} / topic: {}
                """.format(args.servers, args.topic))
            time.sleep(5)


if __name__ == "__main__":
    main()
