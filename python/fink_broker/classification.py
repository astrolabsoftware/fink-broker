# Copyright 2018 AstroLab Software
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
from pyspark.sql.types import StringType

import io
import csv
import requests
import numpy as np
import pandas as pd
from typing import Iterator, Generator, Any

def generate_csv(s: str, lists: list):
    """ Make a string (CSV formatted) given lists of data and header.

    Parameters
    ----------
    s: str
        String which will contain the data.
        Should initially contain the CSV header.
    lists: list of lists
        List containing data.
        Length of `lists` must correspond to the header.

    Returns
    ----------
    s: str
        Updated string with one row per line.
    """
    output = io.StringIO()
    writer = csv.writer(output, quoting=csv.QUOTE_NONNUMERIC)
    _ = [writer.writerow(row) for row in zip(*lists)]
    return s + output.getvalue().replace('\r', '')

def xmatch(
        ra: list, dec: list, id: list,
        extcatalog: str="simbad", distMaxArcsec: int=1):
    """ Build a catalog of (ra, dec, id) in a CSV-like string,
    cross-match with `extcatalog`, and decode the output.

    See http://cdsxmatch.u-strasbg.fr/ for more information.

    Parameters
    ----------
    ra: list of float
        List of RA
    dec: list of float
        List of Dec of the same size as ra.
    id: list of str
        List of object ID (custom)
    extcatalog: str
        Name of the catalog to use for the xMatch.
        See http://cdsxmatch.u-strasbg.fr/ for more information.
    distMaxArcsec: int
        Radius used for searching match. extcatalog sources lying within
        radius of the center (ra, dec) will be considered as matches.

    Returns
    ----------
    data: list of string
        Unformatted decoded data returned by the xMatch
    header: list of string
        Unformatted decoded header returned by the xMatch
    """
    # Build a catalog of alert in a CSV-like string
    table_header = """ra_in,dec_in,objectId\n"""
    table = generate_csv(table_header, [ra, dec, id])

    # Send the request!
    r = requests.post(
         'http://cdsxmatch.u-strasbg.fr/xmatch/api/v1/sync',
         data={
             'request': 'xmatch',
             'distMaxArcsec': distMaxArcsec,
             'selection': 'all',
             'RESPONSEFORMAT': 'csv',
             'cat2': extcatalog,
             'colRA1': 'ra_in',
             'colDec1': 'dec_in'},
         files={'cat1': table})

    # Decode the message, and split line by line
    # First line is header - last is empty
    data = r.content.decode().split("\n")[1:-1]
    header = r.content.decode().split("\n")[0].split(",")

    return data, header

def cross_match_alerts_per_partition(partition: Iterator) -> Generator:
    """ Query the CDSXmatch service to find identified objects
    in alerts. The catalog queried is the SIMBAD bibliographical database.
    We can also use the 10,000+ VizieR tables if needed :-)

    I/O specifically designed for use with `mapPartitions`.

    Parameters
    ----------
    part: Iterator
        Spark partition containing alerts' (ID, ra, dec).

    Returns
    ----------
    out: Generator of tuples
        Tuple with (objectId, ra, dec, name, type).
        If the object is not found in Simbad, name & type
        are marked as Unknown. In the case several objects match
        the centroid of the alert, only the closest is returned.
    """
    # Unwrap partition data
    data = [*partition]
    oid, ra, dec = np.transpose(data)

    data, header = xmatch(ra, dec, oid, extcatalog="simbad", distMaxArcsec=1)

    # Fields of interest (their indices in the output)
    if "main_id" not in header:
        yield []
    else:
        main_id = header.index("main_id")
        main_type = header.index("main_type")
        ra_ind = header.index("ra_in")
        dec_ind = header.index("dec_in")
        oid_ind = header.index("objectId")

        # Get the (ra, dec) of matches
        radec_out = [
            (float(np.array(i.split(","))[ra_ind]),
             float(np.array(i.split(","))[dec_ind])) for i in data]

        # Get the objectId of matches
        id_out = [np.array(i.split(","))[oid_ind] for i in data]

        # Get the names of matches
        names = [np.array(i.split(","))[main_id] for i in data]

        # Get the types of matches
        types = [np.array(i.split(","))[main_type] for i in data]

        # Assign names and types to inputs
        out = []
        for ra_in, dec_in, id_in in zip(ra, dec, oid):
            # cast for picky Spark
            ra_in, dec_in = float(ra_in), float(dec_in)
            id_in = str(id_in)

            # Discriminate with the objectID
            if id_in in id_out:
                # Return the closest object in case of many (smallest angular distance)
                index = id_out.index(id_in)
                out.append((id_in, ra_in, dec_in, str(names[index]), str(types[index])))
            else:
                # Mark as unknown if no match
                out.append((id_in, ra_in, dec_in, "Unknown", "Unknown"))

        yield out

def cross_match_alerts_raw(oid: list, ra: list, dec: list) -> list:
    """ Query the CDSXmatch service to find identified objects
    in alerts. The catalog queried is the SIMBAD bibliographical database.
    We can also use the 10,000+ VizieR tables if needed :-)

    Parameters
    ----------
    oid: list of str
        List containing object ids (custom)
    ra: list of float
        List containing object ra coordinates
    dec: list of float
        List containing object dec coordinates

    Returns
    ----------
    out: List of Tuple
        Each tuple contains (objectId, ra, dec, name, type).
        If the object is not found in Simbad, name & type
        are marked as Unknown. In the case several objects match
        the centroid of the alert, only the closest is returned.
    """
    if len(ra) == 0:
        return []

    data, header = xmatch(ra, dec, oid, extcatalog="simbad", distMaxArcsec=1)

    # Fields of interest (their indices in the output)
    if "main_id" not in header:
        return []
    else:
        main_id = header.index("main_id")
        main_type = header.index("main_type")
        ra_ind = header.index("ra_in")
        dec_ind = header.index("dec_in")
        oid_ind = header.index("objectId")

        # Get the (ra, dec) of matches
        radec_out = [
            (float(np.array(i.split(","))[ra_ind]),
             float(np.array(i.split(","))[dec_ind])) for i in data]

        # Get the objectId of matches
        id_out = [np.array(i.split(","))[oid_ind] for i in data]

        # Get the names of matches
        names = [np.array(i.split(","))[main_id] for i in data]

        # Get the types of matches
        types = [np.array(i.split(","))[main_type] for i in data]

        # Assign names and types to inputs
        out = []
        for ra_in, dec_in, id_in in zip(ra, dec, oid):
            # cast for picky Spark
            ra_in, dec_in = float(ra_in), float(dec_in)
            id_in = str(id_in)

            # Discriminate with the objectID
            if id_in in id_out:
                # Return the closest object in case of many (smallest angular distance)
                index = id_out.index(id_in)
                out.append((id_in, ra_in, dec_in, str(names[index]), str(types[index])))
            else:
                # Mark as unknown if no match
                out.append((id_in, ra_in, dec_in, "Unknown", "Unknown"))

        return out

@pandas_udf(StringType(), PandasUDFType.SCALAR)
def cross_match_alerts_per_batch(id: Any, ra: Any, dec: Any) -> pd.Series:
    """ Query the CDSXmatch service to find identified objects
    in alerts. The catalog queried is the SIMBAD bibliographical database.
    We can also use the 10,000+ VizieR tables if needed :-)

    I/O specifically designed for use as `pandas_udf` in `select` or
    `withColumn` dataframe methods

    Parameters
    ----------
    oid: list of str or Spark DataFrame Column of str
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
    matches = cross_match_alerts_raw(id.values, ra.values, dec.values)
    if len(matches) > 0:
        # (id, ra, dec, name, type)
        # return only the type.
        names = np.transpose(matches)[-1]
    else:
        # Tag as Fail if the request failed.
        names = ["Fail"] * len(id)
    return pd.Series(names)
