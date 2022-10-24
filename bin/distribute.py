#!/usr/bin/env python
# Copyright 2019-2022 AstroLab Software
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
import argparse
import time

from fink_broker.parser import getargs
from fink_broker.sparkUtils import init_sparksession, connect_to_raw_database
from fink_broker.distributionUtils import get_kafka_df
from fink_broker.loggingUtils import get_fink_logger, inspect_application

from fink_utils.spark.utils import concat_col

from fink_utils.spark.utils import apply_user_defined_filter

# User-defined topics
userfilters = [
    'fink_filters.filter_early_sn_candidates.filter.early_sn_candidates',
    'fink_filters.filter_sn_candidates.filter.sn_candidates',
    'fink_filters.filter_sso_ztf_candidates.filter.sso_ztf_candidates',
    'fink_filters.filter_sso_fink_candidates.filter.sso_fink_candidates',
    'fink_filters.filter_kn_candidates.filter.kn_candidates',
    'fink_filters.filter_early_kn_candidates.filter.early_kn_candidates',
    'fink_filters.filter_rate_based_kn_candidates.filter.rate_based_kn_candidates',
    'fink_filters.filter_microlensing_candidates.filter.microlensing_candidates',
    'fink_filters.filter_yso_candidates.filter.yso_candidates'
]

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(name="distribute_{}_{}".format(args.producer, args.night), shuffle_partitions=2)

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    # data path
    scitmpdatapath = args.online_data_prefix + '/science'
    checkpointpath_kafka = args.online_data_prefix + '/kafka_checkpoint'

    # Connect to the TMP science database
    df = connect_to_raw_database(
        scitmpdatapath + "/year={}/month={}/day={}".format(args.night[0:4], args.night[4:6], args.night[6:8]),
        scitmpdatapath + "/year={}/month={}/day={}".format(args.night[0:4], args.night[4:6], args.night[6:8]),
        latestfirst=False
    )

    # Drop partitioning columns
    df = df.drop('year').drop('month').drop('day')

    # Cast fields to ease the distribution
    cnames = df.columns
    cnames[cnames.index('timestamp')] = 'cast(timestamp as string) as timestamp'
    cnames[cnames.index('cutoutScience')] = 'struct(cutoutScience.*) as cutoutScience'
    cnames[cnames.index('cutoutTemplate')] = 'struct(cutoutTemplate.*) as cutoutTemplate'
    cnames[cnames.index('cutoutDifference')] = 'struct(cutoutDifference.*) as cutoutDifference'
    cnames[cnames.index('prv_candidates')] = 'explode(array(prv_candidates)) as prv_candidates'
    cnames[cnames.index('candidate')] = 'struct(candidate.*) as candidate'

    # Retrieve time-series information
    to_expand = [
        'jd', 'fid', 'magpsf', 'sigmapsf',
        'magnr', 'sigmagnr', 'magzpsci', 'isdiffpos'
    ]

    # Append temp columns with historical + current measurements
    prefix = 'c'
    for colname in to_expand:
        df = concat_col(df, colname, prefix=prefix)

    # quick fix for https://github.com/astrolabsoftware/fink-broker/issues/457
    for colname in to_expand:
        df = df.withColumnRenamed('c' + colname, 'c' + colname + 'c')

    broker_list = args.distribution_servers
    for userfilter in userfilters:
        # The topic name is the filter name
        topicname = args.substream_prefix + userfilter.split('.')[-1] + '_ztf'

        # Apply user-defined filter
        df_tmp = apply_user_defined_filter(df, userfilter, logger)

        # Wrap alert data
        df_tmp = df_tmp.selectExpr(cnames)

        # Get the DataFrame for publishing to Kafka (avro serialized)
        df_kafka = get_kafka_df(df_tmp, '')

        # Ensure that the topic(s) exist on the Kafka Server)
        disquery = df_kafka\
            .writeStream\
            .format("kafka")\
            .option("kafka.bootstrap.servers", broker_list)\
            .option("kafka.security.protocol", "SASL_PLAINTEXT")\
            .option("kafka.sasl.mechanism", "SCRAM-SHA-512")\
            .option("topic", topicname)\
            .option("checkpointLocation", checkpointpath_kafka + topicname)\
            .trigger(processingTime='{} seconds'.format(args.tinterval)) \
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
