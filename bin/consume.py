#!/usr/bin/env python
# Copyright 2019 AstroLab Software
# Author: Abhishek Chauhan
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

"""For verifying the working of the distribution pipeline.
Consume the distributed alerts from the Kafka Server.

1. Read from Kafka topic(s)
2. Deserialize the avro data using the pre-defined schema
3. Carry out operations on the obtained DataFrame
"""

import argparse

from fink_broker.parser import getargs
from fink_broker.sparkUtils import init_sparksession, from_avro
from pyspark.sql.functions import col

def main():
    parser = argparse.ArgumentParser(description = __doc__)
    args = getargs(parser)

    # Get or create a Spark Session
    spark = init_sparksession(
        name = "consume_distribution", shuffle_partitions = 2, log_level = "ERROR")

    # The avro schema of the distributed alert
    # Note : This is only for testing purpose
    # ToDo : Reading the schema from a file
    #--------------------------------------------------------------------------#
    # avro_schema_distribution = """
    # {
    #     "doc" : "avro schema for alert distribution",
    #     "name" : "ztf_filtered",
    #     "type" : "record",
    #     "fields" : [
    #         {"name" : "objectId", "type" : "string"},
    #         {"name" : "candid", "type" : "long"},
    #         {"name" : "simbadType", "type" : "string"},
    #         {"name" : "candidate_jd", "type" : "double"},
    #         {"name" : "candidate_pid", "type" : "long"},
    #         {"name" : "candidate_ra", "type" : "double"},
    #         {"name" : "candidate_dec", "type" : "double"}
    #     ]
    #
    # }"""
    #--------------------------------------------------------------------------#
    #--------------------------------------------------------------------------#
    avro_schema_distribution = """
    {
        "name" : "struct",
        "type" : "record",
        "doc" : "avro schema for alert distribution",
        "fields" : [
            {"name" : "objectId", "type" : "string"},
            {"name" : "simbadType", "type" : "string"}
        ]

    }"""
    #--------------------------------------------------------------------------#

    # Topic to read from
    topic = "test_distribution"

    # Read from the Kafka topic
    df_kafka = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9093") \
        .option("subscribe", topic) \
        .load()

    df_kafka.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # Decode the avro data
    df = df_kafka.select(from_avro(df_kafka["value"], avro_schema_distribution).alias("struct"))

    # print one row
    df_kafka.show(3)

    print("\nprinting the schema of the decoded df\n")
    df.printSchema()
    df.select(col("struct.*")).show(3)

if __name__ == "__main__":
    main()
