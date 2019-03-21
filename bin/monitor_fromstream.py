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
from fink_broker.sparkUtils import init_sparksession, connect_with_kafka

from fink_broker.monitoring import monitor_progress_webui

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        'servers', type=str,
        help='Hostname or IP and port of Kafka broker producing stream. [KAFKA_IPPORT]')
    parser.add_argument(
        'topic', type=str,
        help='Name of Kafka topic stream to read from. [KAFKA_TOPIC]')
    parser.add_argument(
        'startingoffsets', type=str,
        help='From which offset you want to start pulling data. [KAFKA_STARTING_OFFSET]')
    parser.add_argument(
        'finkwebpath', type=str,
        help='Folder to store UI data for display. [FINK_UI_PATH]')
    parser.add_argument(
        '-exit_after', type=int, default=None,
        help=""" Stop the service after `exit_after` seconds.
        This primarily for use on Travis, to stop service after some time.
        Use that with `fink start service --exit_after <time>`. Default is None. """)
    args = parser.parse_args()

    # Initialise Spark session
    spark = init_sparksession(
        name="archivingStream", shuffle_partitions=2, log_level="ERROR")

    # Create a streaming dataframe pointing to a Kafka stream
    df = connect_with_kafka(
        servers=args.servers, topic=args.topic,
        startingoffsets=args.startingoffsets, failondataloss=False)

    # Trigger the streaming computation,
    # by defining the sink (memory here) and starting it
    countquery = df \
        .writeStream \
        .queryName("qraw")\
        .format("console")\
        .outputMode("update") \
        .start()

    # Monitor the progress of the stream, and save data for the webUI
    colnames = ["inputRowsPerSecond", "processedRowsPerSecond", "timestamp"]
    monitor_progress_webui(
        countquery,
        2,
        colnames,
        args.finkwebpath)

    # Keep the Streaming running until something or someone ends it!
    if args.exit_after is not None:
        time.sleep(args.exit_after)
        countquery.stop()
        print("Exiting the monitoring service normally...")
    else:
        countquery.awaitTermination()

if __name__ == "__main__":
    main()
