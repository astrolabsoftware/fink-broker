# Copyright 2020 AstroLab Software
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

import numpy as np
import pandas as pd
from astropy.time import Time

from fink_broker.tester import spark_unit_tests

@pandas_udf(TimestampType(), PandasUDFType.SCALAR)
def jd_to_datetime(jd: float):
    """ Convert Julian date into datetime (timestamp)

    Parameters
    ----------
    jd: double
        Julian date

    Returns
    ----------
    out: datetime
        Datetime object

    Examples
    ----------
    >>> from fink_broker.sparkUtils import load_parquet_files
    >>> df = load_parquet_files("archive/raw")
    >>> df.withColumn('datetime', jd_to_datetime(df['candidate.jd']))
    """
    return pd.Series(Time(jd.values, format='jd').to_datetime())

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
    >>> df = load_parquet_files("archive/raw")
    >>> numpart = numPart(df, partition_size=128.)
    >>> print(numpart)
    1
    """
    # Grab the running Spark Session,
    # otherwise create it.
    spark = SparkSession \
        .builder \
        .appName(name) \
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
