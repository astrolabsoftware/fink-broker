# Copyright 2019 AstroLab Software
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
from pyspark.sql.functions import pandas_udf, PandasUDFType, col
from pyspark.sql.types import BooleanType, StructType
from pyspark.sql.functions import struct
from pyspark.sql import DataFrame

import pandas as pd

from typing import Any

from userfilters.levelone import *

from fink_broker.tester import spark_unit_tests

def return_flatten_names(
        df: DataFrame, pref: str = "", flatten_schema: list = []) -> list:
    """From a nested schema (using struct), retrieve full paths for entries
    in the form level1.level2.etc.entry.

    Example, if I have a nested structure such as:
    root
     |-- timestamp: timestamp (nullable = true)
     |-- decoded: struct (nullable = true)
     |    |-- schemavsn: string (nullable = true)
     |    |-- publisher: string (nullable = true)
     |    |-- objectId: string (nullable = true)
     |    |-- candid: long (nullable = true)
     |    |-- candidate: struct (nullable = true)

    It will return a list like
        ["timestamp", "decoded" ,"decoded.schemavsn", "decoded.publisher", ...]

    Parameters
    ----------
    df : DataFrame
        Alert DataFrame
    pref : str, optional
        Internal variable to keep track of the structure, initially sets to "".
    flatten_schema: list, optional
        List containing the names of the flatten schema names.
        Initially sets to [].

    Returns
    -------
    flatten_frame: list
        List containing the names of the flatten schema names.

    Examples
    -------
    >>> df = spark.read.format("parquet").load("archive/alerts_store")
    >>> flatten_schema = return_flatten_names(df)
    >>> assert("decoded.candidate.candid" in flatten_schema)
    """
    if flatten_schema == []:
        for colname in df.columns:
            flatten_schema.append(colname)

    # If the entry is not top level, it is then hidden inside a nested structure
    l_struct_names = [
        i.name for i in df.schema if isinstance(i.dataType, StructType)]

    for l_struct_name in l_struct_names:
        colnames = df.select("{}.*".format(l_struct_name)).columns
        for colname in colnames:
            if pref == "":
                flatten_schema.append(".".join([l_struct_name, colname]))
            else:
                flatten_schema.append(".".join([pref, l_struct_name, colname]))

        # Check if there are other levels nested
        flatten_schema = return_flatten_names(
            df.select("{}.*".format(l_struct_name)),
            pref=l_struct_name,
            flatten_schema=flatten_schema)

    return flatten_schema

def apply_user_defined_filters(df: DataFrame, filter_names: list) -> DataFrame:
    """Apply iteratively user filters to keep only wanted alerts.

    Parameters
    ----------
    df: DataFrame
        Spark DataFrame with alert data
    filter_names: list of string
        List containing filter names to be applied. These filters should
        be functions defined in the folder `userfilters`.

    Returns
    -------
    df: DataFrame
        Spark DataFrame with filtered alert data

    Examples
    -------
    >>> colnames = ["nbad", "rb", "magdiff"]
    >>> df = spark.sparkContext.parallelize(zip(
    ...   [0, 1, 0, 0],
    ...   [0.01, 0.02, 0.6, 0.01],
    ...   [0.02, 0.05, 0.2, 0.01])).toDF(colnames)
    >>> df.show() # doctest: +NORMALIZE_WHITESPACE
    +----+----+-------+
    |nbad|  rb|magdiff|
    +----+----+-------+
    |   0|0.01|   0.02|
    |   1|0.02|   0.05|
    |   0| 0.6|    0.2|
    |   0|0.01|   0.01|
    +----+----+-------+
    <BLANKLINE>


    # Nest the DataFrame as for alerts
    >>> df = df.select(struct(df.columns).alias("candidate"))\
        .select(struct("candidate").alias("decoded"))

    # Apply quality cuts for example (level one)
    >>> assert("qualitycuts" in filter_levelone_names)

    >>> df = apply_user_defined_filters(df, ["qualitycuts"])
    >>> df.select("decoded.candidate.*").show() # doctest: +NORMALIZE_WHITESPACE
    +----+----+-------+
    |nbad|  rb|magdiff|
    +----+----+-------+
    |   0|0.01|   0.02|
    |   0|0.01|   0.01|
    +----+----+-------+
    <BLANKLINE>

    """
    flatten_schema = return_flatten_names(df, pref="", flatten_schema=[])

    # Loop over user-defined filters
    for filter_func_name in filter_names:
        # Note: we could use import_module instead?
        filter_func = globals()[filter_func_name]

        # Note: to access input argument, we need f.func and not just f.
        # This is because f has a decorator on it.
        ninput = filter_func.func.__code__.co_argcount

        # Note: This works only with `struct` fields - not `array`
        argnames = filter_func.func.__code__.co_varnames[:ninput]
        colnames = []
        for argname in argnames:
            colname = [
                col(i) for i in flatten_schema
                if i.endswith(".{}".format(argname))]
            if len(colname) == 0:
                raise AssertionError("""
                    Column name {} is not a valid column of the DataFrame.
                    """)
            colnames.append(colname[0])

        df = df\
            .withColumn("toKeep", filter_func(*colnames))\
            .filter("toKeep == true")\
            .drop("toKeep")

    return df

def apply_user_defined_processors(df: DataFrame, processor_names: list):
    """Apply iteratively user processors to give added values to the stream.

    Each processor will add one new column to the input DataFrame. The name
    of the column will be the name of the processor routine.

    Parameters
    ----------
    df: DataFrame
        Spark DataFrame with alert data
    processor_names: list of string
        List containing processor names to be applied. These processors should
        be functions defined in the folder `userfilters`.

    Returns
    -------
    df: DataFrame
        Spark DataFrame with new columns added.

    Examples
    -------
    >>> df = spark.sparkContext.parallelize(zip(
    ...   [26.8566983, 26.24497],
    ...   [-26.9677112, -26.7569436],
    ...   ["1", "2"])).toDF(["ra", "dec", "objectId"])

    # Nest the DataFrame as for alerts
    >>> df = df.select(struct(df.columns).alias("candidate"))\
        .select(struct("candidate").alias("decoded"))

    >>> assert("cross_match_alerts_per_batch" in processor_levelone_names)

    >>> df = apply_user_defined_processors(df, ["cross_match_alerts_per_batch"])
    >>> new_colnames = ["decoded.candidate.*", "cross_match_alerts_per_batch"]
    >>> df = df.select(new_colnames)
    >>> df.show() # doctest: +NORMALIZE_WHITESPACE
    +----------+-----------+--------+----------------------------+
    |        ra|        dec|objectId|cross_match_alerts_per_batch|
    +----------+-----------+--------+----------------------------+
    |26.8566983|-26.9677112|       1|                        Star|
    |  26.24497|-26.7569436|       2|                     Unknown|
    +----------+-----------+--------+----------------------------+
    <BLANKLINE>

    """
    flatten_schema = return_flatten_names(df, pref="", flatten_schema=[])

    # Loop over user-defined processors
    for processor_func_name in processor_names:
        # Note: we could use import_module instead?
        processor_func = globals()[processor_func_name]

        # Note: to access input argument, we need f.func and not just f.
        # This is because f has a decorator on it.
        ninput = processor_func.func.__code__.co_argcount

        # Note: This works only with `struct` fields - not `array`
        argnames = processor_func.func.__code__.co_varnames[:ninput]
        colnames = []
        for argname in argnames:
            colname = [
                col(i) for i in flatten_schema
                if i.endswith(".{}".format(argname))]
            if len(colname) == 0:
                raise AssertionError("""
                    Column name {} is not a valid column of the DataFrame.
                    """)
            colnames.append(colname[0])

        df = df.withColumn(processor_func.__name__, processor_func(*colnames))

    return df


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    # Run the Spark test suite
    spark_unit_tests(globals())
