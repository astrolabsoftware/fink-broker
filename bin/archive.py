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
"""Store live stream data on disk.
The output can be local FS or distributed FS (e.g. HDFS).
Be careful though to have enough disk space!

For some output sinks where the end-to-end fault-tolerance
can be guaranteed, you will need to specify the location where the system will
write all the checkpoint information. This should be a directory
in an HDFS-compatible fault-tolerant file system.

See also https://spark.apache.org/docs/latest/
structured-streaming-programming-guide.html#starting-streaming-queries
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

import argparse
import json

from fink_broker import avroUtils
from fink_broker.sparkUtils import quiet_logs, from_avro

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
        'outputpath', type=str,
        help='Directory on disk for saving live data. See conf/fink.conf.')
    parser.add_argument(
        'checkpointpath', type=str,
        help="""
        For some output sinks where the end-to-end fault-tolerance
        can be guaranteed, specify the location where the system will
        write all the checkpoint information. This should be a directory
        in an HDFS-compatible fault-tolerant file system.
        See conf/fink.conf & https://spark.apache.org/docs/latest/
        structured-streaming-programming-guide.html#starting-streaming-queries
        """)
    parser.add_argument(
        'finkwebpath', type=str,
        help='Folder to store UI data for display. See conf/fink.conf')
    parser.add_argument(
        'tinterval', type=int,
        help='Time interval between two monitoring. In seconds.')
    args = parser.parse_args()

    # Grab the running Spark Session,
    # otherwise create it.
    spark = SparkSession \
        .builder \
        .appName("classifyStream") \
        .getOrCreate()

    # Set logs to be quieter
    # Put WARN or INFO for debugging, but you will have to dive into
    # a sea of millions irrelevant messages for what you typically need...
    quiet_logs(spark.sparkContext, log_level="ERROR")
    spark.conf.set("spark.streaming.kafka.consumer.cache.enabled", "false")

    # Keep the size of shuffles small
    spark.conf.set("spark.sql.shuffle.partitions", "2")

    # Create a DF from the incoming stream from Kafka
    # Note that <kafka.bootstrap.servers> and <subscribe>
    # must correspond to arguments of the LSST alert system.
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args.servers) \
        .option("subscribe", args.topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Get Schema of alerts
    alert_schema = avroUtils.readSchemaFromAvroFile(
        "schemas/template_schema_ZTF.avro")
    df_schema = spark.read\
        .format("avro")\
        .load("schemas/template_schema_ZTF.avro")\
        .schema
    alert_schema_json = json.dumps(alert_schema)

    # Decode the Avro data, and keep only (timestamp, data)
    df_decoded = df.select(
        [
            "timestamp",
            from_avro(df["value"], alert_schema_json).alias("decoded")
        ]
    )

    # Partition the data hourly
    df_partitionedby = df_decoded\
        .withColumn("year", date_format("timestamp", "yyyy"))\
        .withColumn("month", date_format("timestamp", "MM"))\
        .withColumn("day", date_format("timestamp", "dd"))\
        .withColumn("hour", date_format("timestamp", "hh"))

    # Append new rows every `tinterval` seconds
    countQuery_tmp = df_partitionedby\
        .writeStream\
        .outputMode("append") \
        .format("parquet") \
        .option("checkpointLocation", args.checkpointpath) \
        .option("path", args.outputpath)\
        .partitionBy("year", "month", "day", "hour")

    # Fixed interval micro-batches or ASAP
    if args.tinterval > 0:
        countQuery = countQuery_tmp\
            .trigger(processingTime='{} seconds'.format(args.tinterval)) \
            .start()
        ui_refresh = args.tinterval
    else:
        countQuery = countQuery_tmp.start()
        # Update the UI every 2 seconds to place less load on the browser.
        ui_refresh = 2

    # Monitor the progress of the stream, and save data for the webUI
    colnames = ["inputRowsPerSecond", "processedRowsPerSecond", "timestamp"]
    monitor_progress_webui(
        countQuery,
        ui_refresh,
        colnames,
        args.finkwebpath)

    # Keep the Streaming running until something or someone ends it!
    countQuery.awaitTermination()


if __name__ == "__main__":
    main()
