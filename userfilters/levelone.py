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

# Declare here the filters that will be applied in the
# level one (stream -> raw database)
filter_levelone_names = ["qualitycuts"]

# Declare here the processors that will be applied in the
# level one (stream -> raw database)
processor_levelone_names = [""]

@pandas_udf(BooleanType(), PandasUDFType.SCALAR)
def qualitycuts(nbad: Any, rb: Any, magdiff: Any) -> pd.Series:
    """ Apply simple quality cuts to the alert stream.

    The user will edit this function with his/her needs the following way:

    1) Set the input entry column (i.e. replace nbad,
        rb, etc. by what you need). These must be `candidate` entries.
    2) Update the logic inside the function. The idea is to
        apply conditions based on the values of the columns.
    3) Return a column whose entry is false if the alert has to be discarded,
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

    """
    mask = nbad.values == 0
    mask *= rb.values <= 0.55
    mask *= abs(magdiff.values) <= 0.1

    return pd.Series(mask)
