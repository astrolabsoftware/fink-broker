# Copyright 2020-2024 AstroLab Software
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
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import TimestampType
from pyspark.sql import SparkSession, Column

import numpy as np
import pandas as pd
from astropy.time import Time

from fink_broker.common.tester import spark_unit_tests


def convert_to_millitime(jd: Column, format: str = "jd", now: bool = False) -> Column:
    """Convert date into unix milliseconds (long)

    Parameters
    ----------
    jd: Column
        Julian date column
    format: str, optional
        Astropy time format, e.g. jd, mjd, ... Default is jd.
    now: bool, optional
        If True, return the current time. Default is False.

    Returns
    -------
    out: Column
        Spark Column of UTC timestamps (TimestampType)

    Examples
    --------
    >>> from fink_broker.common.spark_utils import load_parquet_files
    >>> df = load_parquet_files("online/raw/20200101")
    >>> df = df.withColumn('millis', convert_to_millitime(df['candidate.jd']))
    >>> pdf = df.select('millis').toPandas()

    >>> df = df.withColumn('millis', convert_to_millitime(df['candidate.jd'], 'jd', True))
    >>> pdf = df.select('millis').toPandas()
    """

    @pandas_udf(TimestampType())
    def _impl(jd: pd.Series) -> pd.Series:
        if now:
            times = [Time.now().to_datetime()] * len(jd)
        else:
            times = Time(jd.to_numpy(), format=format).to_datetime()
        return pd.Series(times)

    return _impl(jd)


def convert_to_datetime(jd: Column, format: str = "jd") -> Column:
    """Convert date into datetime (timestamp)

    Be careful if you are using this outside Fink. First, you need to check
    you timezone defined in Spark:

    ```
    spark.conf.get("spark.sql.session.timeZone")
    ```

    If this is something else than UTC, then change it:

    ```
    spark.conf.set("spark.sql.session.timeZone", 'UTC')
    ```

    Parameters
    ----------
    jd: Column
        Julian date column
    format: str, optional
        Astropy time format, e.g. jd, mjd, ... Default is jd.

    Returns
    -------
    out: Column
        Datetime column in UTC

    Examples
    --------
    >>> from fink_broker.common.spark_utils import load_parquet_files
    >>> df = load_parquet_files("online/raw/20200101")
    >>> df = df.withColumn('datetime', convert_to_datetime(df['candidate.jd']))
    >>> pdf = df.select('datetime').toPandas()
    """

    @pandas_udf(TimestampType())
    def _impl(jd: pd.Series) -> pd.Series:
        return pd.Series(Time(jd.to_numpy(), format=format).to_datetime())

    return _impl(jd)


def compute_num_part(df, partition_size=128.0):
    """Compute the idle number of partitions of a DataFrame based on its size.

    Parameters
    ----------
    partition_size: float
        Size of a partition in MB

    Returns
    -------
    numpart: int
        Number of partitions of size `partition_size` based
        on the DataFrame size
    partition_size: float, optional
        Size of output partitions in MB

    Examples
    --------
    >>> from fink_broker.common.spark_utils import load_parquet_files
    >>> df = load_parquet_files("online/raw/20200101")
    >>> numpart = compute_num_part(df, partition_size=128.)
    >>> print(numpart)
    1
    """
    # Grab the running Spark Session,
    # otherwise create it.
    spark = SparkSession.builder.getOrCreate()

    logical_plan = df._jdf.queryExecution().logical()
    mode = df._jdf.queryExecution().mode()
    b = (
        spark._jsparkSession.sessionState()
        .executePlan(logical_plan, mode)
        .optimizedPlan()
        .stats()
        .sizeInBytes()
    )

    # Convert in MB
    b_mb = b / 1024.0 / 1024.0

    # Ceil it
    numpart = int(np.ceil(b_mb / partition_size))

    return numpart


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    # Run the Spark test suite
    spark_unit_tests(globals())
