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

import pandas as pd

from typing import Any

from fink_broker.tester import spark_unit_tests

@pandas_udf(BooleanType(), PandasUDFType.SCALAR)
def keep_alert_based_on(nbad: Any, rb: Any, magdiff: Any) -> pd.Series:
    """ Experimental filtering service. For testing purposes only.

    Create a column whose entry is false if the alert has to be discarded,
    and true otherwise.

    Parameters
    ----------
    nbad: Spark DataFrame Column
        Column containing the nbad values
    rb: Spark DataFrame Column
        Column containing the rb values
    magdiff: Spark DataFrame Column
        Column containing the magdiff values

    Returns
    ----------
    out: pandas.Series of bool
        Return a Pandas DataFrame with the appropriate flag: 1 for bad alert,
        and 0 for good alert.

    Examples
    ----------
    >>> df = spark.sparkContext.parallelize(zip(
    ...   [0, 1, 0, 0],
    ...   [0.01, 0.02, 0.6, 0.01],
    ...   [0.02, 0.05, 0.2, 0.01])).toDF(["nbad", "rb", "magdiff"])
    >>> df_type = df.withColumn(
    ...   "tokeep",
    ...   keep_alert_based_on(col("nbad"), col("rb"), col("magdiff")))
    >>> df_type.show() # doctest: +NORMALIZE_WHITESPACE
    +----+----+-------+------+
    |nbad|  rb|magdiff|tokeep|
    +----+----+-------+------+
    |   0|0.01|   0.02|  true|
    |   1|0.02|   0.05| false|
    |   0| 0.6|    0.2| false|
    |   0|0.01|   0.01|  true|
    +----+----+-------+------+
    <BLANKLINE>
    """
    mask = nbad.values == 0
    mask *= rb.values <= 0.55
    mask *= abs(magdiff.values) <= 0.1

    return pd.Series(mask)


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    # Run the Spark test suite
    spark_unit_tests(globals())
