#!/usr/bin/env python
# Copyright 2022-2023 AstroLab Software
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
"""Distribute the elasticc alerts

1. Use the Alert data that is stored in the Science TMP database (Parquet)
2. Apply user defined filters & apply output schema
3. Serialize into Avro
3. Publish to Kafka Topic(s)
"""
import argparse
import time

from pyspark.sql import functions as F

from fink_broker import __version__ as fbvsn
from fink_science import __version__ as fsvsn

from fink_broker.parser import getargs
from fink_broker.sparkUtils import init_sparksession, connect_to_raw_database
from fink_broker.distributionUtils import get_kafka_df
from fink_broker.loggingUtils import get_fink_logger, inspect_application
from fink_broker.partitioning import convert_to_millitime

def format_df_to_elasticc(df):
    """Take the input DataFrame, and format it for ELAsTICC post-processing

    Comments:
    - We currently add non-existing columns on-the-fly. This includes:
        elasticcPublishTimestamp, brokerName, brokerVersion
    - For the `classifications` field, it is not clear to me if we need to
        put one entry per science module, or one single entry that is the
        combination (assuming we know how to combine...) of all module results.
    - We need to define a pandas UDF to extract the classId. Maybe 3 pandas UDF
        because each module will say something different...

    Parameters
    ----------
    df: Spark DataFrame
        DataFrame containing data with the ELAsTICC schema 0.9
    """
    cnames = [
        'alertId', 'diaSource.diaSourceId',
        'elasticcPublishTimestamp', 'brokerIngestTimestamp',
        'brokerName', 'brokerVersion',
        'explode(array(classifications)) as classifications'
    ]

    # Add non existing columns
    df = df.withColumn(
        'elasticcPublishTimestamp',
        convert_to_millitime(
            df['diaSource.midPointTai'],
            F.lit('mjd')
        )
    )
    df = df.withColumn('brokerName', F.lit('Fink'))
    df = df.withColumn('brokerVersion', F.lit('{}'.format(fbvsn)))

    # Schema is struct("classifierName", "classifierParams", "classId", "probability")
    classifications_schema = "array<struct<classifierName:string,classifierParams:string,classId:int,probability:float>>"
    df = df\
        .withColumn(
            'scores',
            F.array(
                df['rf_agn_vs_nonagn'].astype('float'),
                1.0 - df['rf_agn_vs_nonagn'].astype('float'),
                df['snn_snia_vs_nonia'].astype('float'),
                1.0 - df['snn_snia_vs_nonia'].astype('float'),
                df['snn_broad_max_prob'].astype('float'),
                df['cats_fine_max_prob'].astype('float'),
                df['rf_snia_vs_nonia'].astype('float'),
                1.0 - df['rf_snia_vs_nonia'].astype('float'),
            )
        ).withColumn(
            'classes',
            F.array(
                F.lit(221),  # AGN
                F.lit(0),  # AGN Others
                F.lit(111),  # SNN
                F.lit(0),  # SNN Others
                df['snn_broad_class'].astype('int'),
                df['cats_fine_class'].astype('int'),
                F.lit(111),  # EarlySN
                F.lit(0),  # EarlySN Others
            )
        ).withColumn(
            'classifications',
            F.array(
                F.struct(
                    F.lit('AGN classifier'),
                    F.lit('version 2.0'),
                    F.col("classes").getItem(0),
                    F.col("scores").getItem(0)
                ),
                F.struct(
                    F.lit('AGN classifier'),
                    F.lit('version 2.0'),
                    F.col("classes").getItem(1),
                    F.col("scores").getItem(1)
                ),
                F.struct(
                    F.lit('SuperNNova SN Ia classifier'),
                    F.lit('version 1.1'),
                    F.col("classes").getItem(2),
                    F.col("scores").getItem(2)
                ),
                F.struct(
                    F.lit('SuperNNova SN Ia classifier'),
                    F.lit('version 1.1'),
                    F.col("classes").getItem(3),
                    F.col("scores").getItem(3)
                ),
                F.struct(
                    F.lit('SuperNNova broad classifier'),
                    F.lit('version 1.1'),
                    F.col("classes").getItem(4),
                    F.col("scores").getItem(4)
                ),
                F.struct(
                    F.lit('CATS fine classifier'),
                    F.lit('version 3.0'),
                    F.col("classes").getItem(5),
                    F.col("scores").getItem(5)
                ),
                F.struct(
                    F.lit('EarlySN classifier'),
                    F.lit('version 1.0'),
                    F.col("classes").getItem(6),
                    F.col("scores").getItem(6)
                ),
                F.struct(
                    F.lit('EarlySN classifier'),
                    F.lit('version 1.0'),
                    F.col("classes").getItem(7),
                    F.col("scores").getItem(7)
                ),
            ).cast(classifications_schema)
        ).drop("scores").drop("classes")

    return df.selectExpr(cnames)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(
        name="distribute_elasticc_{}".format(args.night),
        shuffle_partitions=2,
        tz='UTC'
    )

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    # data path
    scitmpdatapath = args.online_data_prefix + '/science'
    checkpointpath_kafka = args.online_data_prefix + '/kafka_checkpoint'

    # Connect to the TMP science database
    df = connect_to_raw_database(scitmpdatapath, scitmpdatapath, latestfirst=False)

    # Drop partitioning columns
    df = df.drop('year').drop('month').drop('day')

    broker_list = args.distribution_servers

    # The topic name is the filter name
    topicname = args.substream_prefix + 'desc_elasticc'

    # Wrap alert data
    df = format_df_to_elasticc(df)

    # Get the DataFrame for publishing to Kafka (avro serialized)
    df_kafka = get_kafka_df(df, key='{}_{}'.format(fbvsn, fsvsn), elasticc=True)

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
