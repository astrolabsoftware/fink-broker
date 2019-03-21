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
"""Classify alerts using the xMatch service at CDS.
See http://cdsxmatch.u-strasbg.fr/ for more information.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import argparse
import json
import time

from fink_broker.parser import getargs

from fink_broker.avroUtils import readschemafromavrofile
from fink_broker.sparkUtils import quiet_logs, from_avro
from fink_broker.sparkUtils import write_to_csv
from fink_broker.sparkUtils import init_sparksession, connect_with_kafka
from fink_broker.sparkUtils import get_schemas_from_avro

from fink_broker.classification import cross_match_alerts_per_batch
from fink_broker.monitoring import monitor_progress_webui

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    init_sparksession(
        name="classifyStream", shuffle_partitions=2, log_level="ERROR")

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
            from_avro(df["value"], alert_schema_json).alias("decoded")
        ]
    )

    # Select only (timestamp, id, ra, dec)
    df_expanded = df_decoded.select(
        [
            df_decoded["timestamp"],
            df_decoded["decoded.objectId"],
            df_decoded["decoded.candidate.ra"],
            df_decoded["decoded.candidate.dec"]
        ]
    )

    # for each micro-batch, perform a cross-match with an external catalog,
    # and return the types of the objects (Star, AGN, Unknown, etc.)
    df_type = df_expanded.withColumn(
            "type",
            cross_match_alerts_per_batch(
                col("objectId"),
                col("ra"),
                col("dec")))

    # Group data by type and count members
    df_group = df_type.groupBy("type").count()

    # Update the DataFrame every tinterval seconds
    countquery_tmp = df_group\
        .writeStream\
        .outputMode("complete") \
        .foreachBatch(write_to_csv)

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
        args.finkwebpath)

    # Keep the Streaming running until something or someone ends it!
    if args.exit_after is not None:
        time.sleep(args.exit_after)
        countquery.stop()
        print("Exiting the classify service normally...")
    else:
        countquery.awaitTermination()


if __name__ == "__main__":
    main()
