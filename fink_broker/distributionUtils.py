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

import json
import os
import glob
import shutil
import time

from fink_broker.avroUtils import readschemafromavrofile
from fink_broker.sparkUtils import get_spark_context, to_avro, from_avro
from pyspark.sql import DataFrame
from pyspark.sql.functions import struct, col, lit
from fink_broker.tester import spark_unit_tests
from fink_broker.hbaseUtils import construct_hbase_catalog_from_flatten_schema

import xml.etree.ElementTree as ET
from typing import Tuple

def get_kafka_df(df: DataFrame, schema_path: str) -> DataFrame:
    """Create and return a df to pubish to Kafka

    For a kafka output the dataframe should have the following columns:
    key: (optional) Using a unique key can prevent reading duplicate data
                    as Kafka supports "at least once" write semantics
                    and might result in duplicate writing
    value: (required)
    topic: (*optional)
    *if a topic field is not present in the dataframe it has to be given
    while writng to kafka

    This routine groups the DataFrame columns to be published to Kafka into
    a StructType column and convert it into avro(binary).
    To be able to decode the sent Kafka messages, the avro schema is stored
    at schema_path which is passed as an argument.

    Parameters
    ----------
    df: DataFrame
        A Spark DataFrame created after reading the science database (HBase)
    schema_path: str
        Path where to store the avro schema required for decoding the
        Kafka messages.
    Returns
    ----------
    df: DataFrame
        A Spark DataFrame with an avro(binary) encoded Column named "value"
    ----------
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
    df_kafka = df_struct.select(to_avro("struct").alias("value"))
    save_avro_schema(df, schema_path)

    return df_kafka


def save_avro_schema(df: DataFrame, schema_path: str):
    """Writes the avro schema to a file at schema_path

    This routine checks if an avro schema file exist at the given path
    and creates one if it doesn't.

    To automatically change the schema with changing requirements, ensure to
    delete the schema file at the given path whenever the structure of DF
    read from science db or the contents to be distributed are changed.

    Parameters
    ----------
    df: DataFrame
        A Spark DataFrame
    schema_path: str
        Path where to store the avro schema
    ----------
    """

    # Check if the file exists
    if not os.path.isfile(schema_path):
        # Store the df as an avro file
        path_for_avro = os.path.join(os.environ["PWD"], "flatten_hbase.avro")
        if os.path.exists(path_for_avro):
            shutil.rmtree(path_for_avro)
        df.write.format("avro").save(path_for_avro)

        # Read the avro schema from .avro file
        avro_file = glob.glob(path_for_avro + "/part*")[0]
        avro_schema = readschemafromavrofile(avro_file)

        # Write the schema to a file for decoding Kafka messages
        with open(schema_path, 'w') as f:
            json.dump(avro_schema, f)

        # Remove .avro files and directory
        shutil.rmtree(path_for_avro)


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
    avro(binary). This routine creates a Spark DataFrame with a decoded StructType
    column using the avro schema at schema_path.

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
    >>> df = spark.sparkContext.parallelize(zip(
    ...     ["ZTF18aceatkx", "ZTF18acsbjvw"],
    ...     [697251923115015002, 697251921215010004],
    ...     [20.393772, 20.4233877],
    ...     [-25.4669463, -27.0588511],
    ...     ["Star", "Unknown"])).toDF(["objectId", "candid", "candidate_ra", "candidate_dec", "simbadType"])
    >>> df.show()
    +------------+------------------+------------+-------------+----------+
    |    objectId|            candid|candidate_ra|candidate_dec|simbadType|
    +------------+------------------+------------+-------------+----------+
    |ZTF18aceatkx|697251923115015002|   20.393772|  -25.4669463|      Star|
    |ZTF18acsbjvw|697251921215010004|  20.4233877|  -27.0588511|   Unknown|
    +------------+------------------+------------+-------------+----------+
    <BLANKLINE>
    >>> temp_schema = os.path.join(os.environ["PWD"] + "temp_schema")
    >>> df_kafka = get_kafka_df(df, temp_schema)
    >>> # Decode the avro df
    >>> df_decoded = decode_kafka_df(df_kafka, temp_schema)
    >>> df_decoded.printSchema()
    root
     |-- struct: struct (nullable = true)
     |    |-- objectId: string (nullable = true)
     |    |-- candid: long (nullable = true)
     |    |-- candidate_ra: double (nullable = true)
     |    |-- candidate_dec: double (nullable = true)
     |    |-- simbadType: string (nullable = true)
    <BLANKLINE>
    >>> df_decoded.select(col("struct.*")).show()
    +------------+------------------+------------+-------------+----------+
    |    objectId|            candid|candidate_ra|candidate_dec|simbadType|
    +------------+------------------+------------+-------------+----------+
    |ZTF18aceatkx|697251923115015002|   20.393772|  -25.4669463|      Star|
    |ZTF18acsbjvw|697251921215010004|  20.4233877|  -27.0588511|   Unknown|
    +------------+------------------+------------+-------------+----------+
    <BLANKLINE>
    >>> os.remove(temp_schema)
    """
    # Read the avro schema
    with open(schema_path) as f:
        avro_schema = json.dumps(json.load(f))

    # Decode the avro(binary) column
    df = df_kafka.select(from_avro("value", avro_schema).alias("struct"))

    return df


def update_status_in_hbase(
        df: DataFrame, database_name: str, rowkey: str,
        offsetFile: str, timestamp: int):
    """Update the status column in Hbase

    Parameters
    ----------
    df: DataFrame
        A Spark DataFrame created after reading the database (HBase)
    database_name: str
        Name of the database
    rowkey: str
        Name of the rowkey in the HBase catalog
    offsetFile: str
        the path of offset file for distribution
    timestamp: int
        timestamp till which science db has been scanned and distributed
    ----------
    """
    df = df.select(rowkey, "status")
    df = df.withColumn("status", lit("distributed"))

    update_catalog = construct_hbase_catalog_from_flatten_schema(df.schema,\
                    database_name, rowkey)
    df.write\
      .option("catalog", update_catalog)\
      .format("org.apache.spark.sql.execution.datasources.hbase")\
      .save()

    # write offset(timestamp) to file
    with open(offsetFile, 'w') as f:
        string = "distributed till, {}".format(timestamp)
        f.write(string)


def get_distribution_offset(
        offsetFile: str, startingOffset_dist: str = "latest") -> int:
    """Read and return distribution offset from file

    Parameters
    ----------
    offsetFile: str
        the path of offset file for distribution

    startingOffset_dist: str, optional
        Offset(timestamp) from where to start the distribution. Options are
        latest (read timestamp from file), earliest (from the beginning of time),
        timestamp (custom timestamp input by user)

    Returns
    ----------
    timestamp: int
        a timestamp (typical unix timestamp: time in ms since epoch)

    Examples
    ----------
    # set a test timestamp
    >>> test_t = int(round(time.time() * 1000))

    # write to a file
    >>> with open('dist.offset.test', 'w') as f:
    ...     string = "distributed till, {}".format(test_t)
    ...     f.write(string)
    ...
    31

    # test 1 (wrong file name)
    >>> min_timestamp = get_distribution_offset("invalidFile", "latest")
    >>> min_timestamp
    100

    # test 2 (given earliest)
    >>> min_timestamp = get_distribution_offset("dist.offset.test", "earliest")
    >>> min_timestamp
    100

    # test 3 (given latest)
    >>> min_timestamp = get_distribution_offset("dist.offset.test", "latest")
    >>> min_timestamp == test_t
    True

    # test 4 (no offset given)
    >>> min_timestamp = get_distribution_offset("dist.offset.test")
    >>> min_timestamp == test_t
    True

    # test 5 (custom timestamp given)
    >>> custom_t = int(round(time.time() * 1000))
    >>> min_timestamp = get_distribution_offset("dist.offset.test", custom_t)
    >>> min_timestamp == custom_t
    True

    # Delete offset file
    >>> os.remove('dist.offset.test')

    """
    # if the offset file doesn't exist or is empty
    if not os.path.isfile(offsetFile) or os.path.getsize(offsetFile) <= 0:
        # set a default
        min_timestamp = 100
    else:
        if startingOffset_dist == "latest":
            with open(offsetFile, 'r') as f:
                line = f.readlines()[-1]
                min_timestamp = int(line.split(", ")[-1])

        elif startingOffset_dist == "earliest":
            # set timestamp to beginning of time
            min_timestamp = 100
        else:
            # user given timestamp
            min_timestamp = int(startingOffset_dist)

    return min_timestamp


def parse_distribution_rules(rules_xml: str, df_cols: list) -> Tuple[list, list]:
    """Parse an xml file with rules for filtering

    Parameters
    ----------
    rules_xml: str
        Path to the xml file

    df_cols: list
        List of all the columns in original DataFrame

    Returns
    ----------
    cols_to_distribute: list
        List of all the columns to keep for distribution

    rules_list: list
        List with rules to apply on columns to filter the DataFrame before
        alert distribution

    Examples
    ----------
    """
    # parse the xml and make an element tree
    tree = ET.parse(rules_xml)
    root = tree.getroot()

    #------------------------------------------------------#

    # create a list of columns to select
    cols_to_select = []

    # check if the tree has a select element
    if not ET.iselement(root[0]):
        print("Invalid rules: Select not found")
        return

    # iterate the 'select' element in the xml tree
    for elem in root[0]:
        # get the attributes' dictionary
        attrib = elem.attrib

        # if subcolumn is not given
        if 'subcol' not in attrib:

            # if no subcol exist i.e top-level namespace, select it
            if attrib['name'] in df_cols:
                cols_to_select.append(attrib['name'])

            # else select all sub-columns
            else:
                sub = attrib['name'] + "_"
                cols = [s for s in df_cols if sub in s]
                cols_to_select.extend(cols)

        # when subcolumn is given
        elif 'subcol' in attrib:
            # add the col to select list
            col = attrib['name'] + "_" + attrib['subcol']
            if col in df_cols:
                cols_to_select.append(col)
            else:
                print("Error in Select: invalid column: {}".format(col))
                return

    # remove duplicates
    cols_to_select = list(dict.fromkeys(cols_to_select))

    #------------------------------------------------------#

    # create a list of columns to drop
    cols_to_drop = []

    # check if the tree has a 'drop' element
    if ET.iselement(root[1]):
        # iterate the 'drop' element in the xml tree
        for elem in root[1]:
            # get the attributes' dictionary
            attrib = elem.attrib

            # if subcolumn is not given
            if 'subcol' not in attrib:

                # if no subcol exist i.e top-level namespace
                if attrib['name'] in df_cols:
                    cols_to_drop.append(attrib['name'])

                # else select all sub-columns
                else:
                    sub = attrib['name'] + "_"
                    cols = [s for s in df_cols if sub in s]
                    cols_to_drop.extend(cols)

            # when subcolumn is given
            elif 'subcol' in attrib:
                # add the col to select list
                col = attrib['name'] + "_" + attrib['subcol']
                if col in df_cols:
                    cols_to_drop.append(col)
                else:
                    print("Error in Drop: invalid column: {}".format(col))
                    return

        # remove duplicates
        cols_to_drop = list(dict.fromkeys(cols_to_drop))

    #------------------------------------------------------#

    # obtain columns to distribute
    cols_to_distribute = [e for e in cols_to_select if e not in cols_to_drop]

    #------------------------------------------------------#

    # Create a filtering rules' list
    rules_list = []

    # check if the tree has a 'filter' element
    if ET.iselement(root[2]):
        # iterate the 'filter' element and create a list of rules
        for elem in root[2]:
            # get the attributes' dictionary
            attrib = elem.attrib

            # if subcolumn is not given
            if 'subcol' not in attrib:

                # check if the col exist in top-level namespace
                if attrib['name'] in cols_to_distribute:
                    # create a rule string and append to list
                    rule = attrib['name'] + attrib['operator'] + attrib['value']
                    rules_list.append(rule)
                else:
                    print("can not apply rule to the column {}".format(attrib['name']))

            # when subcolumn is given
            elif 'subcol' in attrib:
                # check for valid column
                col = attrib['name'] + "_" + attrib['subcol']
                if col in cols_to_distribute:
                    # create a rule string and append to list
                    rule = col + attrib['operator'] + attrib['value']
                    rules_list.append(rule)
                else:
                    print("Error in Filter: invalid column: {}".format(col))
                    return

        # remove duplicate rules
        rules_list = list(dict.fromkeys(rules_list))

    #------------------------------------------------------#
    return cols_to_distribute, rules_list


def get_filtered_df(df: DataFrame, rules_xml: str) -> DataFrame:
    """Filter the DataFrame before distribution

    Parameters
    ----------
    df: DataFrame
        A spark DataFrame which is to be filtered

    rules_xml: str
        Path of the xml file defining rules for filtering the DataFrame

    Returns
    ----------
    df: DataFrame
        A filtered DataFrame

    Examples
    ----------
    """
    # Get all the columns in the DataFrame
    df_cols = df.columns

    # Parse the xml file
    cols_to_distribute, rules_list = parse_distribution_rules(rules_xml, df_cols)

    # Obtain the Filtered DataFrame:
    # Select cols to distribute
    df_filtered = df.select(cols_to_distribute)

    # Apply filters
    for rule in rules_list:
        df_filtered = df_filtered.filter(rule)

    return df_filtered


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    # Run the Spark test suite
    spark_unit_tests(globals())
