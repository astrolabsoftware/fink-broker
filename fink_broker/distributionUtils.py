# Copyright 2019-2023 AstroLab Software
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

from fink_broker.sparkUtils import to_avro

from pyspark.sql import DataFrame
from pyspark.sql.functions import struct, lit
from pyspark.sql.avro.functions import to_avro as to_avro_native

from fink_broker.tester import spark_unit_tests


def get_kafka_df(df: DataFrame, key: str, elasticc: bool = False) -> DataFrame:
    """Create and return a df to pubish to Kafka

    For a kafka output the dataframe should have the following columns:
    key: (optional) Using a unique key can prevent reading duplicate data
        as Kafka supports "at least once" write semantics
        and might result in duplicate writing
    value: (required)
    topic: (*optional) if a topic field is not present in the dataframe
        it has to be given while writing to kafka

    This routine groups the DataFrame columns to be published to Kafka into
    a StructType column and convert it into avro(binary).
    To be able to decode the sent Kafka messages, the avro schema is stored
    at schema_path which is passed as an argument.

    Parameters
    ----------
    df: DataFrame
        A Spark DataFrame created after reading the science database (HBase)
    key: str
        Key to decode the alerts.
        Old: fbvsn_fsvsn
        New (>= 2023/01): full schema

    Returns
    -------
    df: DataFrame
        A Spark DataFrame with an avro(binary) encoded Column named "value"
    """
    # Remove the status column before distribution
    cols = df.columns
    if "status" in cols:
        cols.remove("status")

    df = df.select(cols)

    # Create a StructType column in the df for distribution.
    # The contents and schema of the df can change over time
    df_struct = df.select(struct(df.columns).alias("struct"))

    # Convert into avro and save the schema
    if elasticc:
        # The idea is to force the output schema
        # Need better handling of this though...
        jsonschema = open(
            "/home/julien.peloton/elasticc/alert_schema/elasticc.v0_9.brokerClassification.avsc",
            "r",
        ).read()
        df_kafka = df_struct.select(to_avro_native("struct", jsonschema).alias("value"))
    else:
        df_kafka = df_struct.select(to_avro("struct").alias("value"))

    # Add a key based on schema versions
    df_kafka = df_kafka.withColumn("key", lit(key))

    return df_kafka


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    # Run the Spark test suite
    spark_unit_tests(globals())
