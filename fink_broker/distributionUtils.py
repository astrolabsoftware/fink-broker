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
import json
import glob
import subprocess
import tempfile

from fink_broker.avroUtils import readschemafromavrofile
from fink_broker.sparkUtils import to_avro, from_avro

from pyspark.sql import DataFrame
from pyspark.sql.functions import struct, lit
from pyspark.sql.avro.functions import to_avro as to_avro_native

from fink_broker.tester import spark_unit_tests

def get_kafka_df(
        df: DataFrame, key: str, elasticc: bool = False) -> DataFrame:
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
    ----------
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
            '/home/julien.peloton/elasticc/alert_schema/elasticc.v0_9.brokerClassification.avsc',
            'r'
        ).read()
        df_kafka = df_struct.select(to_avro_native("struct", jsonschema).alias("value"))
    else:
        df_kafka = df_struct.select(to_avro("struct").alias("value"))

    # Add a key based on schema versions
    df_kafka = df_kafka.withColumn('key', lit(key))

    return df_kafka

def save_and_load_schema(df: DataFrame, path_for_avro: str) -> str:
    """ Extract AVRO schema from a static Spark DataFrame

    Parameters
    ----------
    df: Spark DataFrame
        Spark dataframe for which we want to extract the schema
    path_for_avro: str
        Temporary path on hdfs where the schema will be written

    Returns
    ----------
    schema: str
        Schema as string
    """
    # save schema
    df.coalesce(1).limit(1).write.format("avro").save(path_for_avro)

    # retrieve data on local disk
    subprocess.run(["hdfs", "dfs", '-get', path_for_avro])

    # Read the avro schema from .avro file
    avro_file = glob.glob(path_for_avro + "/part*")[0]
    avro_schema = readschemafromavrofile(avro_file)

    # Write the schema to a file for decoding Kafka messages
    with open('{}'.format(path_for_avro.replace('.avro', '.avsc')), 'w') as f:
        json.dump(avro_schema, f, indent=2)

    # reload the schema
    with open('{}'.format(path_for_avro.replace('.avro', '.avsc')), 'r') as f:
        schema_ = json.dumps(f.read())

    schema = json.loads(schema_)

    return schema

def decode_kafka_df(df_kafka: DataFrame, schema_path: str) -> DataFrame:
    """Decode the DataFrame read from Kafka

    The DataFrame read from Kafka contains the following columns:
    key: binary
    value: binary
    topic: string
    partition: int
    offset: long
    timestamp: long
    timestampType: integer

    The value column contains the structured data of the alert encoded into
    avro(binary). This routine creates a Spark DataFrame with a decoded
    StructType column using the avro schema at schema_path.

    Parameters
    ----------
    df_kafka: DataFrame
        A Spark DataFrame created after reading the Kafka Source
    schema_path: str
        Path where the avro schema to decode the Kafka message is stored

    Returns
    ----------
    df: DataFrame
        A Spark DataFrame with a StructType Column with decoded data of
        the avro(binary) column named "value"

    Examples
    ----------
    >>> from pyspark.sql.functions import col
    >>> df = spark.sparkContext.parallelize(zip(
    ...     ["ZTF18aceatkx", "ZTF18acsbjvw"],
    ...     [697251923115015002, 697251921215010004],
    ...     [20.393772, 20.4233877],
    ...     [-25.4669463, -27.0588511],
    ...     ["Star", "Unknown"])).toDF([
    ...       "objectId", "candid", "candidate_ra",
    ...       "candidate_dec", "cross_match_alerts_per_batch"])
    >>> df.show()
    +------------+------------------+------------+-------------+----------------------------+
    |    objectId|            candid|candidate_ra|candidate_dec|cross_match_alerts_per_batch|
    +------------+------------------+------------+-------------+----------------------------+
    |ZTF18aceatkx|697251923115015002|   20.393772|  -25.4669463|                        Star|
    |ZTF18acsbjvw|697251921215010004|  20.4233877|  -27.0588511|                     Unknown|
    +------------+------------------+------------+-------------+----------------------------+
    <BLANKLINE>
    >>> temp_schema = os.path.join(os.environ["PWD"], "temp_schema")
    >>> save_avro_schema(df, temp_schema)

    # Encode the data into avro
    >>> df_kafka = get_kafka_df(df, '')

    # Decode the avro df
    >>> df_decoded = decode_kafka_df(df_kafka, temp_schema)
    >>> df_decoded.printSchema()
    root
     |-- struct: struct (nullable = true)
     |    |-- objectId: string (nullable = true)
     |    |-- candid: long (nullable = true)
     |    |-- candidate_ra: double (nullable = true)
     |    |-- candidate_dec: double (nullable = true)
     |    |-- cross_match_alerts_per_batch: string (nullable = true)
    <BLANKLINE>
    >>> df_decoded.select(col("struct.*")).show()
    +------------+------------------+------------+-------------+----------------------------+
    |    objectId|            candid|candidate_ra|candidate_dec|cross_match_alerts_per_batch|
    +------------+------------------+------------+-------------+----------------------------+
    |ZTF18aceatkx|697251923115015002|   20.393772|  -25.4669463|                        Star|
    |ZTF18acsbjvw|697251921215010004|  20.4233877|  -27.0588511|                     Unknown|
    +------------+------------------+------------+-------------+----------------------------+
    <BLANKLINE>
    >>> os.remove(temp_schema)
    """
    # Read the avro schema
    with open(schema_path) as f:
        avro_schema = json.dumps(json.load(f))

    # Decode the avro(binary) column
    df = df_kafka.select(from_avro("value", avro_schema).alias("struct"))

    return df


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    # Run the Spark test suite
    spark_unit_tests(globals())
