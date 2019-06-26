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
from pyspark.sql import DataFrame

import os
import pandas as pd
import xml.etree.ElementTree as ET

from typing import Any, Tuple

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


def parse_xml_rules(rules_xml: str, df_cols: list) -> Tuple[list, list]:
    """Parse an xml file with rules for filtering

    Parameters
    ----------
    rules_xml: str
        Path to the xml file

    df_cols: list
        List of all the columns in original DataFrame

    Returns
    ----------
    cols_to_distribute: list
        List of all the columns to keep for distribution

    rules_list: list
        List with rules to apply on columns to filter the DataFrame before
        alert distribution

    Examples
    ----------
    # Set path to xml rule file
    >>> rules_xml = os.path.abspath(os.path.join(
    ...         os.environ['FINK_HOME'], 'fink_broker/test_files/distribution-rules-sample.xml'))

    # get list of all columns in the dataframe
    >>> df_cols = ["objectId", "candid", "candidate_jd", "candidate_ra",
    ...        "candidate_dec", "candidate_magpsf", "simbadType",
    ...        "cutoutScience_fileName", "cutoutScience_stampData"]

    # get columns to distribute and rules to apply
    >>> cols_to_distribute, rules_list = parse_xml_rules(rules_xml, df_cols)

    # Print
    >>> for x in cols_to_distribute:
    ...     print(x)
    objectId
    candidate_ra
    candidate_dec
    candidate_magpsf
    simbadType

    >>> for rule in rules_list:
    ...     print(rule)
    candidate_magpsf>16
    candidate_ra<22
    simbadType='Star'

    # given an empty xml file
    >>> cols_to_distribute, rules_list = parse_xml_rules('invalid_xml', df_cols)
    invalid xml: does not exist or is empty

    # invalid column definition in 'select'
    >>> rules_xml_test1 = os.path.abspath(os.path.join(
    ...         os.environ['FINK_HOME'], 'fink_broker/test_files/distribution-rules-test1.xml'))
    >>> cols_to_distribute, rules_list = parse_xml_rules(rules_xml_test1, df_cols)
    Error in Select: invalid column: candidate_pid

    # invalid column definition in 'drop'
    >>> rules_xml_test2 = os.path.abspath(os.path.join(
    ...         os.environ['FINK_HOME'], 'fink_broker/test_files/distribution-rules-test2.xml'))
    >>> cols_to_distribute, rules_list = parse_xml_rules(rules_xml_test2, df_cols)
    Error in Drop: invalid column: candidate_pid

    # invalid column definition in 'filter'
    >>> rules_xml_test3 = os.path.abspath(os.path.join(
    ...         os.environ['FINK_HOME'], 'fink_broker/test_files/distribution-rules-test3.xml'))
    >>> cols_to_distribute, rules_list = parse_xml_rules(rules_xml_test3, df_cols)
    Error in Filter: invalid column: candidate_fid
    """
    # if the xml file doesn't exist or is empty
    if not os.path.isfile(rules_xml) or os.path.getsize(rules_xml) <= 0:
        print("invalid xml: does not exist or is empty")
        return [],[]

    # parse the xml and make an element tree
    tree = ET.parse(rules_xml)
    root = tree.getroot()

    #------------------------------------------------------#

    # create a list of columns to select
    cols_to_select = []

    # iterate the 'select' element in the xml tree
    for elem in root[0]:
        # get the attributes' dictionary
        attrib = elem.attrib

        # if subcolumn is not given
        if 'subcol' not in attrib:

            # if no subcol exist i.e top-level namespace, select it
            if attrib['name'] in df_cols:
                cols_to_select.append(attrib['name'])

            # else select all sub-columns
            else:
                sub = attrib['name'] + "_"
                cols = [s for s in df_cols if sub in s]
                cols_to_select.extend(cols)

        # when subcolumn is given
        elif 'subcol' in attrib:
            # add the col to select list
            col = attrib['name'] + "_" + attrib['subcol']
            if col in df_cols:
                cols_to_select.append(col)
            else:
                print("Error in Select: invalid column: {}".format(col))
                return [],[]

    # remove duplicates
    cols_to_select = list(dict.fromkeys(cols_to_select))

    #------------------------------------------------------#

    # create a list of columns to drop
    cols_to_drop = []

    # check if the tree has a 'drop' element
    if ET.iselement(root[1]):
        # iterate the 'drop' element in the xml tree
        for elem in root[1]:
            # get the attributes' dictionary
            attrib = elem.attrib

            # if subcolumn is not given
            if 'subcol' not in attrib:

                # if no subcol exist i.e top-level namespace
                if attrib['name'] in df_cols:
                    cols_to_drop.append(attrib['name'])

                # else select all sub-columns
                else:
                    sub = attrib['name'] + "_"
                    cols = [s for s in df_cols if sub in s]
                    cols_to_drop.extend(cols)

            # when subcolumn is given
            elif 'subcol' in attrib:
                # add the col to select list
                col = attrib['name'] + "_" + attrib['subcol']
                if col in df_cols:
                    cols_to_drop.append(col)
                else:
                    print("Error in Drop: invalid column: {}".format(col))
                    return [],[]

        # remove duplicates
        cols_to_drop = list(dict.fromkeys(cols_to_drop))

    #------------------------------------------------------#

    # obtain columns to distribute
    cols_to_distribute = [e for e in cols_to_select if e not in cols_to_drop]

    #------------------------------------------------------#

    # Create a filtering rules' list
    rules_list = []

    # check if the tree has a 'filter' element
    if ET.iselement(root[2]):
        # iterate the 'filter' element and create a list of rules
        for elem in root[2]:
            # get the attributes' dictionary
            attrib = elem.attrib

            # if subcolumn is not given
            if 'subcol' not in attrib:

                # check if the col exist in top-level namespace
                if attrib['name'] in cols_to_distribute:
                    # create a rule string and append to list
                    rule = attrib['name'] + attrib['operator'] + attrib['value']
                    rules_list.append(rule)
                else:
                    print("can not apply rule to the column {}".format(attrib['name']))
                    return cols_to_distribute, []

            # when subcolumn is given
            elif 'subcol' in attrib:
                # check for valid column
                col = attrib['name'] + "_" + attrib['subcol']
                if col in cols_to_distribute:
                    # create a rule string and append to list
                    rule = col + attrib['operator'] + attrib['value']
                    rules_list.append(rule)
                else:
                    print("Error in Filter: invalid column: {}".format(col))
                    return cols_to_distribute, []

        # remove duplicate rules
        rules_list = list(dict.fromkeys(rules_list))

    #------------------------------------------------------#
    return cols_to_distribute, rules_list


def filter_df_using_xml(df: DataFrame, rules_xml: str) -> DataFrame:
    """Filter the DataFrame before distribution

    Parameters
    ----------
    df: DataFrame
        A spark DataFrame which is to be filtered

    rules_xml: str
        Path of the xml file defining rules for filtering the DataFrame

    Returns
    ----------
    df: DataFrame
        A filtered DataFrame

    Examples
    ----------
    >>> df = spark.sparkContext.parallelize(zip(
    ...     ["ZTF18aceatkx", "ZTF18acsbjvw", "ZTF18acsbten"],
    ...     [697251923115015002, 697251921215010004, 697252386115010006],
    ...     [2458451.7519213, 2458451.7519213, 2458451.7523843],
    ...     [20.393772, 20.4233877, 12.5489498],
    ...     [-25.4669463, -27.0588511, -13.7619586],
    ...     [16.074839, 17.245092, 19.667372],
    ...     ["Star", "Unknown", "Unknown"])).toDF(["objectId", "candid", "candidate_jd",
    ...     "candidate_ra", "candidate_dec", "candidate_magpsf", "simbadType"])
    >>> df.show()
    +------------+------------------+---------------+------------+-------------+----------------+----------+
    |    objectId|            candid|   candidate_jd|candidate_ra|candidate_dec|candidate_magpsf|simbadType|
    +------------+------------------+---------------+------------+-------------+----------------+----------+
    |ZTF18aceatkx|697251923115015002|2458451.7519213|   20.393772|  -25.4669463|       16.074839|      Star|
    |ZTF18acsbjvw|697251921215010004|2458451.7519213|  20.4233877|  -27.0588511|       17.245092|   Unknown|
    |ZTF18acsbten|697252386115010006|2458451.7523843|  12.5489498|  -13.7619586|       19.667372|   Unknown|
    +------------+------------------+---------------+------------+-------------+----------------+----------+
    <BLANKLINE>

    # Set path to xml rule file
    >>> rules_xml = os.path.abspath(os.path.join(
    ...         os.environ['FINK_HOME'], 'fink_broker/test_files/distribution-rules-sample.xml'))

    # get filtered dataframe
    >>> df_filtered = filter_df_using_xml(df, rules_xml)
    >>> df_filtered.show()
    +------------+------------+-------------+----------------+----------+
    |    objectId|candidate_ra|candidate_dec|candidate_magpsf|simbadType|
    +------------+------------+-------------+----------------+----------+
    |ZTF18aceatkx|   20.393772|  -25.4669463|       16.074839|      Star|
    +------------+------------+-------------+----------------+----------+
    <BLANKLINE>
    """
    # Get all the columns in the DataFrame
    df_cols = df.columns

    # Parse the xml file
    cols_to_distribute, rules_list = parse_xml_rules(rules_xml, df_cols)

    # Obtain the Filtered DataFrame:
    # Select cols to distribute
    df_filtered = df.select(cols_to_distribute)

    # Apply filters
    for rule in rules_list:
        df_filtered = df_filtered.filter(rule)

    return df_filtered.cache()


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    # Run the Spark test suite
    spark_unit_tests(globals())
