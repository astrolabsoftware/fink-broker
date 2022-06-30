#!/usr/bin/env python
# Copyright 2019-2022 AstroLab Software
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
from pyspark.sql import functions as F

import fastavro
import fastavro.schema
import argparse
import time
import io

from fink_broker.parser import getargs

from fink_broker.sparkUtils import from_avro
from fink_broker.sparkUtils import init_sparksession, connect_to_kafka
from fink_broker.sparkUtils import get_schemas_from_avro
from fink_broker.loggingUtils import get_fink_logger, inspect_application
from fink_broker.partitioning import convert_to_datetime, convert_to_millitime

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(name="stream2raw", shuffle_partitions=2)

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    # data path
    rawdatapath = args.online_data_prefix + '/raw'
    checkpointpath_raw = args.online_data_prefix + '/raw_checkpoint'

    # Create a streaming dataframe pointing to a Kafka stream
    kerberos = 'public2.alerts.ztf' in args.servers
    df = connect_to_kafka(
        servers=args.servers,
        topic=args.topic,
        startingoffsets=args.startingoffsets_stream,
        failondataloss=False,
        kerberos=kerberos)

    # Get Schema of alerts
    if 'elasticc' not in args.topic:
        alert_schema, _, alert_schema_json = get_schemas_from_avro(args.schema)

    # Decode the Avro data, and keep only (timestamp, data)
    if '134.158.' in args.servers or 'localhost' in args.servers:
        # using custom from_avro (not available for Spark 2.4.x)
        # it will be available from Spark 3.0 though
        df_decoded = df.select(
            [
                from_avro(df["value"], alert_schema_json).alias("decoded")
            ]
        )
    elif 'elasticc' in args.topic:
        schema = fastavro.schema.load_schema(args.schema)
        alert_schema_json = fastavro.schema.to_parsing_canonical_form(schema)
        df_decoded = df.select(
            [
                from_avro(df["value"], alert_schema_json).alias("decoded")
            ]
        )
    elif 'public2.alerts.ztf' in args.servers:
        # Decode on-the-fly using fastavro
        f = F.udf(lambda x: next(fastavro.reader(io.BytesIO(x))), alert_schema)
        df_decoded = df.select(
            [
                f(df['value']).alias("decoded")
            ]
        )
    else:
        msg = "Data source {} is not known - a decoder must be set".format(
            args.servers)
        logger.warn(msg)
        spark.stop()

    # Flatten the data columns to match the incoming alert data schema
    cnames = df_decoded.columns
    cnames[cnames.index('decoded')] = 'decoded.*'
    df_decoded = df_decoded.selectExpr(cnames)

    # Partition the data hourly
    if 'candidate' in df_decoded.columns:
        timecol = 'candidate.jd'
        converter = lambda x: convert_to_datetime(x)
    elif 'diaSource' in df_decoded.columns:
        timecol = 'diaSource.midPointTai'
        converter = lambda x: convert_to_datetime(x, F.lit('mjd'))

        # Add ingestion timestamp
        df_decoded = df_decoded.withColumn(
            'brokerIngestTimestamp',
            convert_to_millitime(
                df_decoded['candidate.jd'],
                F.lit('jd'),
                F.lit(True)
            )
        )

    df_partitionedby = df_decoded\
        .withColumn("timestamp", converter(df_decoded[timecol]))\
        .withColumn("year", F.date_format("timestamp", "yyyy"))\
        .withColumn("month", F.date_format("timestamp", "MM"))\
        .withColumn("day", F.date_format("timestamp", "dd"))

    # Append new rows every `tinterval` seconds
    # and drop duplicates see fink-broker/issues/443
    if 'candid' in df_decoded.columns:
        idcol = 'candid'
        df_partitionedby = df_partitionedby.dropDuplicates([idcol])

    countquery_tmp = df_partitionedby\
        .writeStream\
        .outputMode("append") \
        .format("parquet") \
        .option("checkpointLocation", checkpointpath_raw) \
        .option("path", rawdatapath)\
        .partitionBy("year", "month", "day")

    # Fixed interval micro-batches or ASAP
    if args.tinterval > 0:
        countquery = countquery_tmp\
            .trigger(processingTime='{} seconds'.format(args.tinterval)) \
            .start()
    else:
        countquery = countquery_tmp.start()

    # Keep the Streaming running until something or someone ends it!
    if args.exit_after is not None:
        time.sleep(args.exit_after)
        countquery.stop()
        logger.info("Exiting the stream2raw service normally...")
    else:
        countquery.awaitTermination()


if __name__ == "__main__":
    main()
