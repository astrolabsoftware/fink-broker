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
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import BooleanType, StringType

import pandas as pd
import numpy as np

from typing import Any

# Declare here the filters that will be applied in the
# level two (redistribution)
filter_leveltwo_names = ["dist_stream_cut"]

@pandas_udf(BooleanType(), PandasUDFType.SCALAR)
def dist_stream_cut(sigmapsf: Any, sgscore1: Any) -> pd.Series:
    """ Apply user defined cuts to the distribution stream.

    The user will edit this function (or create a new filtering function)
    with his/her needs the following way:

    1) Set the input entry column (i.e. replace sigmapsf,
        sgscore1, etc. by what you need). These must be `candidate` entries.
    2) Update the logic inside the function. The idea is to
        apply conditions based on the values of the columns.
    3) Return a column whose entry is false if the alert has to be discarded,
    and true otherwise.

    Parameters
    ----------
    sigmapsf: Spark DataFrame Column
        Column containing the sigmapsf values
    sgscore1: Spark DataFrame Column
        Column containing the sgscore1 values

    Returns
    ----------
    out: pandas.Series of bool
        Return a Pandas DataFrame with the appropriate flag: 0 for bad alert,
        and 1 for good alert.

    """
    mask = sigmapsf.values < 0.1
    mask *= sgscore1.values >= 0.75

    return pd.Series(mask)
