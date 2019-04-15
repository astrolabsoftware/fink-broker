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
import time

from fink_broker.parser import getargs

from fink_broker.avroUtils import readschemafromavrofile
from fink_broker.sparkUtils import quiet_logs, from_avro
from fink_broker.sparkUtils import init_sparksession, connect_with_kafka
from fink_broker.sparkUtils import get_schemas_from_avro

from fink_broker.monitoring import monitor_progress_webui

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    init_sparksession(
        name="archivingStream", shuffle_partitions=2, log_level="ERROR")

    # Create a streaming dataframe pointing to a Kafka stream
    df = connect_with_kafka(
        servers=args.servers, topic=args.topic,
        startingoffsets=args.startingoffsets, failondataloss=False)

    # Get Schema of alerts
    _, _, alert_schema_json = get_schemas_from_avro(args.schema)

    # Decode the Avro data, and keep only (timestamp, data)
    df_decoded = df.select(
        [
            "timestamp",
            "topic",
            from_avro(df["value"], alert_schema_json).alias("decoded")
        ]
    )

    # Partition the data hourly
    df_partitionedby = df_decoded\
        .withColumn("year", date_format("timestamp", "yyyy"))\
        .withColumn("month", date_format("timestamp", "MM"))\
        .withColumn("day", date_format("timestamp", "dd"))\
        .withColumn("hour", date_format("timestamp", "HH"))

    # Append new rows every `tinterval` seconds
    countquery_tmp = df_partitionedby\
        .writeStream\
        .outputMode("append") \
        .format("parquet") \
        .option("checkpointLocation", args.checkpointpath) \
        .option("path", args.outputpath)\
        .partitionBy("topic", "year", "month", "day", "hour")

    # Fixed interval micro-batches or ASAP
    if args.tinterval > 0:
        countquery = countquery_tmp\
            .trigger(processingTime='{} seconds'.format(args.tinterval)) \
            .start()
        ui_refresh = args.tinterval
    else:
        countquery = countquery_tmp.start()
        # Update the UI every 2 seconds to place less load on the browser.
        ui_refresh = 2

    # Monitor the progress of the stream, and save data for the webUI
    colnames = ["inputRowsPerSecond", "processedRowsPerSecond", "timestamp"]
    monitor_progress_webui(
        countquery,
        ui_refresh,
        colnames,
        args.finkwebpath, "live.csv")

    # Keep the Streaming running until something or someone ends it!
    if args.exit_after is not None:
        time.sleep(args.exit_after)
        countquery.stop()
        print("Exiting the archiving service normally...")
    else:
        countquery.awaitTermination()


if __name__ == "__main__":
    main()
