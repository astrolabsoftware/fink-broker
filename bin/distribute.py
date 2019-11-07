#!/usr/bin/env python
# Copyright 2019 AstroLab Software
# Author: Abhishek Chauhan, Julien Peloton
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

"""Distribute the alerts to users

1. Use the Alert data that is stored in the Science TMP database (Parquet)
2. Apply user defined filters
3. Serialize into Avro
3. Publish to Kafka Topic(s)
"""
from pyspark.sql.functions import lit

import argparse
import time

from fink_broker.parser import getargs
from fink_broker.sparkUtils import init_sparksession, connect_to_raw_database
from fink_broker.distributionUtils import get_kafka_df
from fink_broker.filters import apply_user_defined_filters
from fink_broker.loggingUtils import get_fink_logger, inspect_application

from userfilters.leveltwo import filter_leveltwo_names

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(name="distribute", shuffle_partitions=2)

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    # Connect to the TMP science database
    df = connect_to_raw_database(
        args.scitmpdatapath, args.scitmpdatapath + "/*", latestfirst=False)

    # Apply level two filters
    df = apply_user_defined_filters(df, filter_leveltwo_names)

    # Switch publisher
    df = df.withColumn('publisher-tmp', lit('Fink')) \
        .drop('publisher') \
        .withColumnRenamed('publisher-tmp', 'publisher')

    # Cast fields to ease the distribution
    cnames = df.columns
    cnames[cnames.index('timestamp')] = 'cast(timestamp as string) as timestamp'
    cnames[cnames.index('cutoutScience')] = 'struct(cutoutScience.*) as cutoutScience'
    cnames[cnames.index('cutoutTemplate')] = 'struct(cutoutTemplate.*) as cutoutTemplate'
    cnames[cnames.index('cutoutDifference')] = 'struct(cutoutDifference.*) as cutoutDifference'
    cnames[cnames.index('prv_candidates')] = 'explode(array(prv_candidates)) as prv_candidates'
    cnames[cnames.index('candidate')] = 'struct(candidate.*) as candidate'
    df = df.selectExpr(cnames)

    # Drop partitioning columns
    df = df.drop('year').drop('month').drop('day').drop('hour')

    # Get the DataFrame for publishing to Kafka (avro serialized)
    df_kafka = get_kafka_df(df, '')

    # Get topic name to publish on
    topic = args.distribution_topic
    broker_list = args.distribution_servers

    # Ensure that the topic(s) exist on the Kafka Server)
    disquery = df_kafka\
        .writeStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", broker_list)\
        .option("kafka.security.protocol", "SASL_PLAINTEXT")\
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")\
        .option("topic", topic)\
        .option("checkpointLocation", args.checkpointpath_kafka)\
        .start()

    # Keep the Streaming running until something or someone ends it!
    if args.exit_after is not None:
        time.sleep(args.exit_after)
        disquery.stop()
        logger.info("Exiting the distribute service normally...")
    else:
        # Wait for the end of queries
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
