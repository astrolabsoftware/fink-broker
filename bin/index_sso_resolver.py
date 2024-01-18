#!/usr/bin/env python
# Copyright 2023 AstroLab Software
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
"""Construct SSO index table, and push to HBase
"""
import os
import argparse
import datetime

import pandas as pd
import numpy as np

from fink_broker.sparkUtils import init_sparksession
from fink_broker.hbaseUtils import push_to_hbase
from fink_broker.loggingUtils import get_fink_logger, inspect_application

from fink_spins.ssoft import rockify


def mark_as_duplicate(series, count=0):
    """ Iteratively tag duplicates

    Parameters
    ----------
    series: pd.Series
        Pandas series containing strings
    count: int, optional
        Counter, starting at 0.

    Returns
    ----------
    series: pd.Series
        Input series with duplicate markers of
        the form _{level of duplicate}

    Examples
    ----------
    >>> pdf = pd.DataFrame({'col': [['a', 'b', 'c'], ['a', 'b'], ['b', 'c']]}).explode('col')
    >>> mark_as_duplicate(pdf['col'].copy())
    0    a_0
    0    b_0
    0    c_0
    1    a_1
    1    b_1
    2    b_2
    2    c_1
    Name: col, dtype: object
    """
    if count == 0:
        # initialise markers
        series = series.apply(lambda x: x + "-{}".format(count))

    mask = series.duplicated(keep='first')
    if np.sum(mask) > 0:
        # duplicates exists
        series[mask] = series[mask].apply(
            lambda x: x.replace('-{}'.format(count), '-{}'.format(count + 1))
        )
        return mark_as_duplicate(series.copy(), count + 1)
    else:
        return series


def sort_indices(pdf, column='ssodnet'):
    """ Isolate decimal values in `column`, and sort the DataFrame by ascending order

    Parameters
    ----------
    pdf: pd.DataFrame
        Pandas DataFrame
    column: str
        Column name

    Returns
    ----------
    pdf: pd.DataFrame
        Sorted DataFrame
    """
    is_decimal = pdf[column].apply(lambda x: x.isdecimal())

    pdf1 = pdf[is_decimal]
    pdf1[column] = pdf1[column].astype(int)
    pdf1 = pdf1.sort_values(column, ascending=True)
    pdf1[column] = pdf1[column].astype(str)

    pdf2 = pdf[~is_decimal]

    pdf = pd.concat((pdf1, pdf2))

    return pdf


def main():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        '-version', type=str, default=None,
        help="""
        Version to use in the format YYYY.MM
        Default is None, meaning current Year.Month
        """
    )
    parser.add_argument(
        '-science_db_name', type=str, default='ztf',
        help="""
        The prefix of the HBase table
        """
    )
    args = parser.parse_args(None)

    if args.version is None:
        now = datetime.datetime.now()
        version = '{}.{:02d}'.format(now.year, now.month)
    else:
        version = args.version

    # Initialise Spark session
    spark = init_sparksession(
        name="index_sso_resolver_{}".format(version),
        shuffle_partitions=2
    )

    logger = get_fink_logger(spark.sparkContext.appName, "INFO")

    # debug statements
    inspect_application(logger)

    # This is generated by generate_ssoft.py
    df = spark.read.format('parquet').load('sso_aggregated_{}'.format(version))

    pdf = df.select('ssnamenr').toPandas()

    sso_name, sso_number = rockify(pdf.ssnamenr.copy())

    # fill None with values from original ssnamenr
    has_no_name = np.equal(sso_name, None)
    sso_name[has_no_name] = pdf.ssnamenr[has_no_name]

    # Keep only valid number
    is_valid_number = np.equal(sso_number, sso_number)
    sso_number_valid = sso_number[is_valid_number]

    # Vector contains (MPC_names, MPC_numbers, ZTF_ssnamenr)
    index_ssodnet = np.concatenate((sso_name, sso_number_valid, pdf.ssnamenr.values))

    # create index vector for Fink
    index_fink = np.concatenate(
        (
            pdf.ssnamenr.values,
            pdf.ssnamenr[is_valid_number].values,
            pdf.ssnamenr.values
        )
    )
    index_name = np.concatenate((sso_name, sso_name[is_valid_number], sso_name))
    index_number = np.concatenate((sso_number, sso_number_valid, sso_number))

    msg = """
    Number of (unique) SSO objects in Fink: {:,}

    STATISTIC FROM QUAERO
    -------------------------------------
    Number of numbered objects: {:,}
    --> Number of objects with no number: {:,}
        --> Number of objects with only prov. designation: {:,}
        --> Number of objects unindentified by quaero: {:,}
    """.format(
        len(pdf),
        len(sso_number_valid),
        len(sso_number) - len(sso_number_valid),
        np.sum(~has_no_name * ~is_valid_number),
        np.sum(has_no_name)
    )

    logger.info(msg)

    pdf_index = pd.DataFrame(
        {
            'ssodnet': index_ssodnet,
            'ssnamenr': index_fink,
            'name': index_name,
            'number': index_number
        },
        dtype=str
    )

    pdf_index = sort_indices(pdf_index)

    index_col = mark_as_duplicate(pdf_index['ssodnet'].copy())
    pdf_index['ssodnet'] = index_col

    # Make the ssodnet lower case (case-insensitive)
    pdf_index['ssodnet'] = pdf_index['ssodnet'].apply(lambda x: x.lower())

    df_index = spark.createDataFrame(pdf_index)

    cf = {i: 'i' for i in df_index.columns}

    index_row_key_name = 'ssodnet'
    index_name = '.sso_resolver'

    push_to_hbase(
        df=df_index,
        table_name=args.science_db_name + index_name,
        rowkeyname=index_row_key_name,
        cf=cf,
        catfolder=os.environ['FINK_HOME']
    )


if __name__ == "__main__":
    main()