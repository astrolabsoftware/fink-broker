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
from pyspark.sql.types import BooleanType, StringType

import pandas as pd
import numpy as np

from fink_broker.classification import cross_match_alerts_raw

from typing import Any

# Declare here the filters that will be applied in the
# level one (raw -> science database)
filter_levelone_names = ["qualitycuts"]

# Declare here the processors that will be applied in the
# level one (stream -> raw database)
processor_levelone_names = ["cross_match_alerts_per_batch"]

@pandas_udf(BooleanType(), PandasUDFType.SCALAR)
def qualitycuts(nbad: Any, rb: Any, magdiff: Any) -> pd.Series:
    """ Apply simple quality cuts to the alert stream.

    The user will edit this function (or create a new filtering function)
    with his/her needs the following way:

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
        Return a Pandas DataFrame with the appropriate flag: false for bad alert,
        and true for good alert.

    """
    mask = nbad.values == 0
    mask *= rb.values >= 0.55
    mask *= abs(magdiff.values) <= 0.1

    return pd.Series(mask)

@pandas_udf(StringType(), PandasUDFType.SCALAR)
def cross_match_alerts_per_batch(objectId: Any, ra: Any, dec: Any) -> pd.Series:
    """ Query the CDSXmatch service to find identified objects
    in alerts. The catalog queried is the SIMBAD bibliographical database.
    We can also use the 10,000+ VizieR tables if needed :-)

    I/O specifically designed for use as `pandas_udf` in `select` or
    `withColumn` dataframe methods

    The user will create a new processing function with his/her needs the
    following way:

    1) Define the input entry column. These must be `candidate` entries.
    2) Update the logic inside the function. The idea is to
        apply conditions based on the values of the columns.
    3) Return a column with added value after processing

    Parameters
    ----------
    objectId: list of str or Spark DataFrame Column of str
        List containing object ids (custom)
    ra: list of float or Spark DataFrame Column of float
        List containing object ra coordinates
    dec: list of float or Spark DataFrame Column of float
        List containing object dec coordinates

    Returns
    ----------
    out: pandas.Series of string
        Return a Pandas DataFrame with the type of object found in Simbad.
        If the object is not found in Simbad, the type is
        marked as Unknown. In the case several objects match
        the centroid of the alert, only the closest is returned.
        If the request Failed (no match at all), return Column of Fail.

    """
    matches = cross_match_alerts_raw(objectId.values, ra.values, dec.values)

    # For regular alerts, the number of matches is always non-zero as
    # alerts with no counterpart will be labeled as Unknown.
    # If cross_match_alerts_raw returns a zero-length list of matches, it is
    # a sign of a problem (logged).
    if len(matches) > 0:
        # (objectId, ra, dec, name, type)
        # return only the type.
        names = np.transpose(matches)[-1]
    else:
        # Tag as Fail if the request failed.
        names = ["Fail"] * len(objectId)
    return pd.Series(names)
