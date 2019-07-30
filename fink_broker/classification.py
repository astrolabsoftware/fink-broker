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
from pyspark.sql.types import StringType
import io
import csv
import logging
import requests
import numpy as np
import pandas as pd
from typing import Any
from astroquery.simbad import Simbad
import astropy.coordinates as coord
from fink_broker.tester import spark_unit_tests
import astropy.units as u


def generate_csv(s: str, lists: list) -> str:
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

    Examples
    ----------
    >>> header = "toto,tata\\n"
    >>> lists = [[1, 2], ["cat", "dog"]]
    >>> table = generate_csv(header, lists)
    >>> print(table)
    toto,tata
    1,"cat"
    2,"dog"
    <BLANKLINE>
    """
    output = io.StringIO()
    writer = csv.writer(output, quoting=csv.QUOTE_NONNUMERIC)
    _ = [writer.writerow(row) for row in zip(*lists)]
    return s + output.getvalue().replace('\r', '')


def xmatch(
        ra: list, dec: list, id: list,
        extcatalog: str = "simbad", distmaxarcsec: int = 1) -> (list, list):
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
    distmaxarcsec: int
        Radius used for searching match. extcatalog sources lying within
        radius of the center (ra, dec) will be considered as matches.

    Returns
    ----------
    data: list of string
        Unformatted decoded data returned by the xMatch
    header: list of string
        Unformatted decoded header returned by the xmatch

    """
    # Build a catalog of alert in a CSV-like string
    table_header = """ra_in,dec_in,objectId\n"""
    table = generate_csv(table_header, [ra, dec, id])

    # Send the request!
    r = requests.post(
        'http://cdsxmatch.u-strasbg.fr/xmatch/api/v1/sync',
        data={
            'request': 'xmatch',
            'distMaxArcsec': distmaxarcsec,
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


def xmatch_slow(
        ra: list, dec: list, id: list,
        distmaxarcsec: int = 1) -> (pd.DataFrame):
    """ Build a catalog of (ra, dec, id) as pandas DataFrame,
    cross-match using astroquery module of SIMBAD, and decode the output.

    See https://astroquery.readthedocs.io/ for more information.

    Parameters
    ----------
    ra: list of float
        List of RA
    dec: list of float
        List of Dec of the same size as ra.
    id: list of str
        List of object ID (custom)
    distmaxarcsec: int
        Radius used for searching match. extcatalog sources lying within
        radius of the center (ra, dec) will be considered as matches.

    Returns
    ----------
    data_filt_new: pd.DataFrame
        Formatted decoded data returned by the astroquery module
    """

    # Reset the fields to a minimal number
    Simbad.reset_votable_fields()
    # Add new fields to the query
    Simbad.add_votable_fields('otype')
    Simbad.add_votable_fields('ra(d)')
    Simbad.add_votable_fields('dec(d)')

    list_keys = ['MAIN_ID', 'RA_d', 'DEC_d', 'OTYPE']
    list_old_keys = ['main_id', 'ra', 'dec', 'main_type']
    dictionary_simbad_to_new = dict(zip(list_keys, list_old_keys))

    # create a mask with the entries of the query
    nans = [np.nan for a in range(0, len(ra))]
    mask = pd.DataFrame(zip(nans, ra, dec, nans, id))
    mask.columns = list_old_keys + ['objectId']

    # Send requests in vector form and obtain a table as a result
    units = tuple([u.deg, u.deg])
    query_new = (
        Simbad.query_region(
            coord.SkyCoord(
                ra=ra,
                dec=dec,
                unit=units
            ), radius=u.deg / 3600.
        )
    )

    if query_new is not None:
        # if table not empy, convert it to a pandas DataFrame
        data_new = query_new[list_keys].to_pandas()\
            .rename(columns=dictionary_simbad_to_new)

        # convert from bytes to ascii
        data_new['main_id'] = data_new['main_id'].values[0].decode('ascii')
        data_new['main_type'] = data_new['main_type'].values[0].decode('ascii')

        # locate object id rounding to the first 3 digits
        place_objid = data_new['dec'].round(4).values == mask['dec']\
            .round(4).values
        data_new['objectId'] = mask['objectId'].loc[place_objid]

        # create a complementary mask
        complementary_mask = data_new['dec'].round(4).values != mask['dec']\
            .round(4).values

        # concatenate table with mask if needed
        data_filt_new = pd.concat([mask.loc[complementary_mask], data_new])\
            .replace(np.nan, 'Unknown')

        # sort if needed
        data_filt_new = data_filt_new.sort_values(by=['objectId'])

    else:
        logging.warning("Empty query - setting xmatch to Unknown")
        data_filt_new = mask.replace(np.nan, 'Unknown')\
            .sort_values(by=['objectId'])

    return data_filt_new


def refine_search(
        ra: list, dec: list, oid: list,
        id_out: list, names: list, types: list) -> list:
    """ Create a final table by merging coordinates of objects found on the
    bibliographical database, with those objects which were not found.

    Parameters
    ----------
    ra: list of float
        List of RA
    dec: list of float
        List of Dec of the same size as ra.
    oid: list of str
        List of object ID (custom)
    id_out: list of str
        List of object ID returned by the xmatch with CDS
    names: list of str
        For matches, names of the celestial objects found
    types: list of str
        For matches, astronomical types of the celestial objects found

    Returns
    ----------
    out: List of Tuple
        Each tuple contains (objectId, ra, dec, name, type).
        If the object is not found in Simbad, name & type
        are marked as Unknown. In the case several objects match
        the centroid of the alert, only the closest is returned.
    """
    out = []
    for ra_in, dec_in, id_in in zip(ra, dec, oid):
        # cast for picky Spark
        ra_in, dec_in = float(ra_in), float(dec_in)
        id_in = str(id_in)

        # Discriminate with the objectID
        if id_in in id_out:
            # Return the closest object in case of many
            # (smallest angular distance)
            index = id_out.index(id_in)
            out.append((
                id_in, ra_in, dec_in,
                str(names[index]), str(types[index])))

        else:
            # Mark as unknown if no match
            out.append((id_in, ra_in, dec_in, "Unknown", "Unknown"))

    return out


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

    Examples
    ----------
    >>> ra = [26.8566983, 26.24497]
    >>> dec = [-26.9677112, -26.7569436]
    >>> id = ["1", "2"]
    >>> objects = cross_match_alerts_raw(id, ra, dec)
    >>> print(objects) # doctest: +NORMALIZE_WHITESPACE
    [('1', 26.8566983, -26.9677112, 'TYC 6431-115-1', 'Star'),
     ('2', 26.24497, -26.7569436, 'Unknown', 'Unknown')]
    """
    if len(ra) == 0:
        return []

    # Catch TimeoutError and ConnectionError
    try:
        data, header = xmatch(
            ra, dec, oid, extcatalog="simbad", distmaxarcsec=1)
    except (ConnectionError, TimeoutError) as ce:
        logging.warning("XMATCH failed " + repr(ce))
        return []

    # Fields of interest (their indices in the output)
    main_id = header.index("main_id")
    main_type = header.index("main_type")
    oid_ind = header.index("objectId")

    # Get the objectId of matches
    id_out = [np.array(i.split(","))[oid_ind] for i in data]

    # Get the names of matches
    names = [np.array(i.split(","))[main_id] for i in data]

    # Get the types of matches
    types = [np.array(i.split(","))[main_type] for i in data]

    # Assign names and types to inputs
    out = refine_search(ra, dec, oid, id_out, names, types)

    return out


def cross_match_alerts_raw_slow(oid: list, ra: list, dec: list) -> list:
    """ Query the CDSXmatch service to find identified objects
    in alerts. The catalog queried is the SIMBAD database using the
    astroquery module, as an alternative to the xmatch method.

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

    Examples
    ----------
    >>> ra = [26.8566983, 26.24497]
    >>> dec = [-26.9677112, -26.7569436]
    >>> id = ["1", "2"]
    >>> objects = cross_match_alerts_raw_slow(id, ra, dec)
    >>> print(objects) # doctest: +NORMALIZE_WHITESPACE
    [('1', 26.8566983, -26.9677112, 'TYC 6431-115-1', 'Star'),
    ('2', 26.24497, -26.7569436, 'Unknown', 'Unknown')]
    """

    if len(ra) == 0:
        return []

    data_new = xmatch_slow(ra, dec, oid, distmaxarcsec=1)

    # Fields of interest (their indices in the output)
    if "main_id" not in data_new.columns:
        return []

    else:
        main_id = 'main_id'
        main_type = 'main_type'
        oid_ind = 'objectId'

        # Get the objectId of matches
        id_out = list(data_new[oid_ind].values)

        # Get the names of matches
        names = data_new[main_id].values

        # Get the types of matches
        types = data_new[main_type].values

        # Assign names and types to inputs
        out = refine_search(ra, dec, oid, id_out, names, types)

    return out


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    # Run the Spark test suite
    spark_unit_tests(globals())
