#!/usr/bin/env python
# Copyright 2018 AstroLab Software
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

from fink_broker.sparkUtils import quiet_logs
from fink_broker.monitoring import monitor_progress_webui

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

    # Set logs to be quieter
    # Put WARN or INFO for debugging, but you will have to dive into
    # a sea of millions irrelevant messages for what you typically need...
    quiet_logs(spark.sparkContext, log_level="ERROR")

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
        .format("console")\
        .outputMode("update") \
        .start()

    # Monitor the progress of the stream, and save data for the webUI
    colnames = ["inputRowsPerSecond", "processedRowsPerSecond", "timestamp"]
    monitor_progress_webui(
        countQuery,
        2,
        colnames,
        args.finkwebpath)

    # Keep the Streaming running until something or someone ends it!
    countQuery.awaitTermination()

if __name__ == "__main__":
    main()
