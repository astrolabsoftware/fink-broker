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
from pyspark.sql.functions import col
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

import os
import importlib

from fink_broker.tester import spark_unit_tests
from fink_broker.loggingUtils import get_fink_logger

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
    >>> df = spark.read.format("parquet").load("online/raw")
    >>> flatten_schema = return_flatten_names(df)
    >>> assert("candidate.candid" in flatten_schema)
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

def apply_user_defined_filter(df: DataFrame, toapply: str) -> DataFrame:
    """Apply a user filter to keep only wanted alerts.

    Parameters
    ----------
    df: DataFrame
        Spark DataFrame with alert data
    toapply: string
        Filter name to be applied. It should be in the form
        module.module.routine (see example below).

    Returns
    -------
    df: DataFrame
        Spark DataFrame with filtered alert data

    Examples
    -------
    >>> from pyspark.sql.functions import struct
    >>> colnames = ["cdsxmatch", "rb", "magdiff"]
    >>> df = spark.sparkContext.parallelize(zip(
    ...   ['RRLyr', 'Unknown', 'Star', 'SN1a'],
    ...   [0.01, 0.02, 0.6, 0.01],
    ...   [0.02, 0.05, 0.1, 0.01])).toDF(colnames)
    >>> df.show() # doctest: +NORMALIZE_WHITESPACE
    +---------+----+-------+
    |cdsxmatch|  rb|magdiff|
    +---------+----+-------+
    |    RRLyr|0.01|   0.02|
    |  Unknown|0.02|   0.05|
    |     Star| 0.6|    0.1|
    |     SN1a|0.01|   0.01|
    +---------+----+-------+
    <BLANKLINE>


    # Nest the DataFrame as for alerts
    >>> df = df.select(struct(df.columns).alias("candidate"))\
        .select(struct("candidate").alias("decoded"))

    # Apply quality cuts for example (level one)
    >>> toapply = 'fink_filters.filter_rrlyr.filter.rrlyr'
    >>> df = apply_user_defined_filter(df, toapply)
    >>> df.select("decoded.candidate.*").show() # doctest: +NORMALIZE_WHITESPACE
    +---------+----+-------+
    |cdsxmatch|  rb|magdiff|
    +---------+----+-------+
    |    RRLyr|0.01|   0.02|
    +---------+----+-------+
    <BLANKLINE>

    # Using a wrong filter name will lead to an error
    >>> df = apply_user_defined_filter(
    ...   df, "unknownfunc") # doctest: +SKIP
    """
    logger = get_fink_logger(__name__, "INFO")

    flatten_schema = return_flatten_names(df, pref="", flatten_schema=[])

    # Load the filter
    filter_name = toapply.split('.')[-1]
    module_name = toapply.split('.' + filter_name)[0]
    module = importlib.import_module(module_name)
    filter_func = getattr(module, filter_name, None)

    # Note: to access input argument, we need f.func and not just f.
    # This is because f has a decorator on it.
    ninput = filter_func.func.__code__.co_argcount

    # Note: This works only with `struct` fields - not `array`
    argnames = filter_func.func.__code__.co_varnames[:ninput]
    colnames = []
    for argname in argnames:
        colname = [
            col(i) for i in flatten_schema
            if i.endswith("{}".format(argname))]
        if len(colname) == 0:
            raise AssertionError("""
                Column name {} is not a valid column of the DataFrame.
                """.format(argname))
        colnames.append(colname[0])

    logger.info(
        "new filter/topic registered: {} from {}".format(
            filter_name, module_name))

    return df\
        .withColumn("toKeep", filter_func(*colnames))\
        .filter("toKeep == true")\
        .drop("toKeep")


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    # Run the Spark test suite
    spark_unit_tests(globals())
