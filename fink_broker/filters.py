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
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import struct

import pandas as pd

from typing import Any

from userfilters.levelone import *

from fink_broker.tester import spark_unit_tests

def apply_user_defined_filters(df, filter_names):
    """Apply iteratively user filters to keep only wanted alerts.

    Parameters
    ----------
    df: DataFrame
        Spark DataFrame with alert data
    filter_names: list of string
        List containing filter name to be applied. These filters should
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
    >>> df = apply_user_defined_filters(df, filter_levelone_names)
    >>> df.select("decoded.candidate.*").show() # doctest: +NORMALIZE_WHITESPACE
    +----+----+-------+
    |nbad|  rb|magdiff|
    +----+----+-------+
    |   0|0.01|   0.02|
    |   0|0.01|   0.01|
    +----+----+-------+
    <BLANKLINE>

    """
    # Loop over user-defined filters
    for filter_func_name in filter_names:
        # Note: we could use import_module instead?
        filter_func = globals()[filter_func_name]

        # Note: to access input argument, we need f.func and not just f.
        # This is because f has a decorator on it.
        ninput = filter_func.func.__code__.co_argcount

        # Note: This works only with `candidate` fields.
        # TODO: Make it general.
        colnames = [
            col("decoded.candidate.{}".format(i))
            for i in filter_func.func.__code__.co_varnames[:ninput]]

        df = df\
            .withColumn("toKeep", filter_func(*colnames))\
            .filter("toKeep == true")\
            .drop("toKeep")

    return df


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    # Run the Spark test suite
    spark_unit_tests(globals())
