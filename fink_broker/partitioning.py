# Copyright 2020-2022 AstroLab Software
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
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import TimestampType
from pyspark.sql import SparkSession

import numpy as np
import pandas as pd
from astropy.time import Time

from fink_broker.tester import spark_unit_tests

@pandas_udf(TimestampType(), PandasUDFType.SCALAR)
def convert_to_datetime(jd: pd.Series, format=None, scale=None) -> pd.Series:
    """ Convert date into datetime (timestamp)

    Parameters
    ----------
    jd: double
        Julian date
    format: str
        Astropy time format, e.g. jd, mjd, ...
    scale: str
        Time scale: utc, tai, ...

    Returns
    ----------
    out: datetime
        Datetime object

    Examples
    ----------
    >>> from fink_broker.sparkUtils import load_parquet_files
    >>> df = load_parquet_files("online/raw")
    >>> df = df.withColumn('datetime', convert_to_datetime(df['candidate.jd']))
    >>> pdf = df.select('datetime').toPandas()
    """
    if format is None:
        formatval = 'jd'
    else:
        formatval = format.values[0]

    if scale is None:
        scaleval = 'utc'
    else:
        scaleval = scale.values[0]

    return pd.Series(Time(jd.values, format=formatval, scale=scaleval).to_datetime(timezone=scaleval))

def numPart(df, partition_size=128.):
    """ Compute the idle number of partitions of a DataFrame
    based on its size.

    Parameters
    ----------
    partition_size: float
        Size of a partition in MB

    Returns
    ----------
    numpart: int
        Number of partitions of size `partition_size` based
        on the DataFrame size
    partition_size: float, optional
        Size of output partitions in MB

    Examples
    ----------
    >>> from fink_broker.sparkUtils import load_parquet_files
    >>> df = load_parquet_files("online/raw")
    >>> numpart = numPart(df, partition_size=128.)
    >>> print(numpart)
    1
    """
    # Grab the running Spark Session,
    # otherwise create it.
    spark = SparkSession \
        .builder \
        .getOrCreate()

    b = spark._jsparkSession\
        .sessionState()\
        .executePlan(df._jdf.queryExecution().logical())\
        .optimizedPlan()\
        .stats()\
        .sizeInBytes()

    # Convert in MB
    b_mb = b / 1024. / 1024.

    # Ceil it
    numpart = int(np.ceil(b_mb / partition_size))

    return numpart


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    # Run the Spark test suite
    spark_unit_tests(globals())
