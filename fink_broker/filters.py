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


def parse_xml_rules(xml_file: str, df_cols: list) -> Tuple[list, list]:
    """Parse an xml file with rules for filtering

    Parameters
    ----------
    xml_file: str
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
    candidate_magpsf > 16
    candidate_ra < 22
    simbadType = 'Star'

    # given an empty xml file
    >>> cols_to_distribute, rules_list = parse_xml_rules('invalid_xml', df_cols)
    invalid xml file

    # invalid column definition in 'select'
    >>> rules_xml_test1 = os.path.abspath(os.path.join(
    ...         os.environ['FINK_HOME'], 'fink_broker/test_files/distribution-rules-test1.xml'))
    >>> cols_to_distribute, rules_list = parse_xml_rules(rules_xml_test1, df_cols)
    Invalid column: candidate_pid

    # invalid column definition in 'drop'
    >>> rules_xml_test2 = os.path.abspath(os.path.join(
    ...         os.environ['FINK_HOME'], 'fink_broker/test_files/distribution-rules-test2.xml'))
    >>> cols_to_distribute, rules_list = parse_xml_rules(rules_xml_test2, df_cols)
    Invalid column: candidate_pid

    # invalid column definition in 'filter'
    >>> rules_xml_test3 = os.path.abspath(os.path.join(
    ...         os.environ['FINK_HOME'], 'fink_broker/test_files/distribution-rules-test3.xml'))
    >>> cols_to_distribute, rules_list = parse_xml_rules(rules_xml_test3, df_cols)
    Can't apply rule: invalid column: candidate_fid
    """
    # check if the file exists and isn't empty
    if not os.path.isfile(xml_file) or os.path.getsize(xml_file) <= 0:
        print("invalid xml file")
        return [],[]

    # parse xml file and make element tree
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # method to get columns
    def get_columns(node: Any, df_cols: list):

        cols = []
        for elem in node:
            attrib = elem.attrib
            col = attrib['name']

            if 'subcol' in attrib:
                col += "_" + attrib['subcol']

                if col not in df_cols:
                    print(f"Invalid column: {col}")
                    return []

                cols.append(col)

            elif col in df_cols:
                cols.append(col)

            else:
                col += "_"
                col_list = [x for x in df_cols if col in x]
                cols.extend(col_list)

        # remove duplicates
        cols = list(dict.fromkeys(cols))
        return cols

    # method to get rules
    def get_rules(node: Any, cols: list):

        rules = []
        for elem in node:
            attrib = elem.attrib
            col = attrib['name']

            if 'subcol' in attrib:
                col += "_" + attrib['subcol']

                if col not in cols:
                    print(f"Can't apply rule: invalid column: {col}")
                    return []

                rule = col + " " + attrib['operator'] + " " + attrib['value']
                rules.append(rule)

            elif col in cols:
                rule = col + " " + attrib['operator'] + " " + attrib['value']
                rules.append(rule)
            else:
                print(f"To apply rule, please select subcol for: {col}")
                return []

        # remove duplicates
        rules = list(dict.fromkeys(rules))
        return rules

    cols_to_select = []
    cols_to_drop = []
    rules_list = []

    # 'select' is present
    if ET.iselement(root[0]):
        cols_to_select = get_columns(root[0], df_cols)

    # 'drop' is present and cols_to_select isn't empty
    if ET.iselement(root[1]) and cols_to_select:
        cols_to_drop = get_columns(root[1], df_cols)

    cols_to_distribute = [c for c in cols_to_select if c not in cols_to_drop]

    # 'filter' is present and cols_to_distribute isn't empty
    if ET.iselement(root[2]) and cols_to_distribute:
        rules_list = get_rules(root[2], cols_to_distribute)

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
