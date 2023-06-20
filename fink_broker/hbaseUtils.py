# Copyright 2019-2023 AstroLab Software
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
import os
import json
import logging

import numpy as np

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException

from fink_broker import __version__ as fbvsn
from fink_science import __version__ as fsvsn

from fink_science.t2.utilities import T2_COLS
from fink_science.xmatch.utils import MANGROVE_COLS

from fink_broker.tester import spark_unit_tests

_LOG = logging.getLogger(__name__)

def load_fink_cols():
    """ Fink-derived columns used in HBase tables with type.

    Returns
    ---------
    out: dictionary
        Keys are column names (flattened). Values are data type.

    Examples
    --------
    >>> out = load_fink_cols()
    >>> print(len(out))
    34
    """
    fink_cols = {
        'DR3Name': {'type': 'string', 'default': 'Unknown'},
        'Plx': {'type': 'float', 'default': 0.0},
        'anomaly_score': {'type': 'double', 'default': 0.0},
        'cdsxmatch': {'type': 'string', 'default': 'Unknown'},
        'e_Plx': {'type': 'float', 'default': 0.0},
        'gcvs': {'type': 'string', 'default': 'Unknown'},
        'mulens': {'type': 'double', 'default': 0.0},
        'nalerthist': {'type': 'int', 'default': 0},
        'rf_kn_vs_nonkn': {'type': 'double', 'default': 0.0},
        'rf_snia_vs_nonia': {'type': 'double', 'default': 0.0},
        'roid': {'type': 'int', 'default': 0},
        'snn_sn_vs_all': {'type': 'double', 'default': 0.0},
        'snn_snia_vs_nonia': {'type': 'double', 'default': 0.0},
        'tracklet': {'type': 'string', 'default': ''},
        'vsx': {'type': 'string', 'default': 'Unknown'},
        'x3hsp': {'type': 'string', 'default': 'Unknown'},
        'x4lac': {'type': 'string', 'default': 'Unknown'}
    }

    # Update dictionary with nested cols
    # for col_ in MANGROVE_COLS:
    #     name = F.col('mangrove.{}'.format(col_)).alias('mangrove_{}'.format(col_))
    #     fink_cols.update(
    #         {name: {'type': 'string', 'default': 'None'}}
    #     )
    fink_nested_cols = {}
    for col_ in MANGROVE_COLS:
        name = 'mangrove.{}'.format(col_)
        fink_nested_cols.update(
            {name: {'type': 'string', 'default': 'None'}}
        )

    for col_ in T2_COLS:
        name = 't2.{}'.format(col_)
        fink_nested_cols.update(
            {name: {'type': 'float', 'default': 0.0}}
        )

    return fink_cols, fink_nested_cols

def load_all_cols():
    """ Fink/ZTF columns used in HBase tables with type.

    Returns
    ---------
    out: dictionary
        Keys are column names (flattened). Values are data type.

    Examples
    --------
    >>> out = load_all_cols()
    >>> print(len(out))
    143
    """
    fink_cols, fink_nested_cols = load_fink_cols()

    root_level = {
        'fink_broker_version': 'string',
        'fink_science_version': 'string',
        'objectId': 'string',
        'publisher': 'string',
        'candid': 'long',
        'schemavsn': 'string',
    }

    candidates = {
        'aimage': 'float',
        'aimagerat': 'float',
        'bimage': 'float',
        'bimagerat': 'float',
        'chinr': 'float',
        'chipsf': 'float',
        'classtar': 'float',
        'clrcoeff': 'float',
        'clrcounc': 'float',
        'clrmed': 'float',
        'clrrms': 'float',
        'dec': 'double',
        'decnr': 'double',
        'diffmaglim': 'float',
        'distnr': 'float',
        'distpsnr1': 'float',
        'distpsnr2': 'float',
        'distpsnr3': 'float',
        'drb': 'float',
        'drbversion': 'string',
        'dsdiff': 'float',
        'dsnrms': 'float',
        'elong': 'float',
        'exptime': 'float',
        'fid': 'int',
        'field': 'int',
        'fwhm': 'float',
        'isdiffpos': 'string',
        'jd': 'double',
        'jdendhist': 'double',
        'jdendref': 'double',
        'jdstarthist': 'double',
        'jdstartref': 'double',
        'magap': 'float',
        'magapbig': 'float',
        'magdiff': 'float',
        'magfromlim': 'float',
        'maggaia': 'float',
        'maggaiabright': 'float',
        'magnr': 'float',
        'magpsf': 'float',
        'magzpsci': 'float',
        'magzpscirms': 'float',
        'magzpsciunc': 'float',
        'mindtoedge': 'float',
        'nbad': 'int',
        'ncovhist': 'int',
        'ndethist': 'int',
        'neargaia': 'float',
        'neargaiabright': 'float',
        'nframesref': 'int',
        'nid': 'int',
        'nmatches': 'int',
        'nmtchps': 'int',
        'nneg': 'int',
        'objectidps1': 'long',
        'objectidps2': 'long',
        'objectidps3': 'long',
        'pdiffimfilename': 'string',
        'pid': 'long',
        'programid': 'int',
        'programpi': 'string',
        'ra': 'double',
        'ranr': 'double',
        'rb': 'float',
        'rbversion': 'string',
        'rcid': 'int',
        'rfid': 'long',
        'scorr': 'double',
        'seeratio': 'float',
        'sgmag1': 'float',
        'sgmag2': 'float',
        'sgmag3': 'float',
        'sgscore1': 'float',
        'sgscore2': 'float',
        'sgscore3': 'float',
        'sharpnr': 'float',
        'sigmagap': 'float',
        'sigmagapbig': 'float',
        'sigmagnr': 'float',
        'sigmapsf': 'float',
        'simag1': 'float',
        'simag2': 'float',
        'simag3': 'float',
        'sky': 'float',
        'srmag1': 'float',
        'srmag2': 'float',
        'srmag3': 'float',
        'ssdistnr': 'float',
        'ssmagnr': 'float',
        'ssnamenr': 'string',
        'ssnrms': 'float',
        'sumrat': 'float',
        'szmag1': 'float',
        'szmag2': 'float',
        'szmag3': 'float',
        'tblid': 'long',
        'tooflag': 'int',
        'xpos': 'float',
        'ypos': 'float',
        'zpclrcov': 'float',
        'zpmed': 'float'
    }

    candidates = {'candidate.' + k: v for k, v in candidates.items()}

    images = {
        'cutoutScience.stampData': 'binary',
        'cutoutTemplate.stampData': 'binary',
        'cutoutDifference.stampData': 'binary'
    }

    return root_level, candidates, images, fink_cols, fink_nested_cols

def bring_to_current_schema(df):
    """ Check all columns exist, fill if necessary, and cast data

    Parameters
    ----------
    df: DataFrame
        Spark DataFrame with raw alert data

    Returns
    ----------
    out: DataFrame
        Spark DataFrame with HBase data structure
    """
    root_level, candidates, images, fink_cols, fink_nested_cols = load_all_cols()

    tmp_i = []
    tmp_d = []
    tmp_b = []

    # assuming no missing columns
    for colname, coltype in root_level.items():
        tmp_i.append(F.col(colname).cast(coltype))

    # assuming no missing columns
    for colname, coltype in candidates.items():
        tmp_i.append(F.col(colname).cast(coltype))

    cols_i = df.select(tmp_i).columns

    # assuming no missing columns
    for colname, coltype in images.items():
        name = F.col(colname).alias(colname.replace('.', '_')).cast(coltype)
        tmp_b.append(name)

    cols_b = df.select(tmp_b).columns

    # check all columns exist, otherwise create it
    for colname, coltype_and_default in fink_cols.items():
        try:
            # ony here to check if the column exists
            df.select(colname)
        except AnalysisException:
            _LOG.warn("Missing columns detected in the DataFrame: {}".format(colname))
            _LOG.warn("Adding a new column with value `{}` and type `{}`".format(coltype_and_default['default'], coltype_and_default['type']))
            df = df.withColumn(colname, F.lit(coltype_and_default['default']))
        tmp_d.append(F.col(colname).cast(coltype_and_default['type']))

    # check all columns exist, otherwise create it
    for colname, coltype_and_default in fink_nested_cols.items():
        try:
            # ony here to check if the column exists
            df.select(colname)

            # rename root.level into root_level
            name = F.col(colname).alias(colname.replace('.', '_')).cast(coltype_and_default['type'])
            tmp_d.append(name)
        except AnalysisException:
            _LOG.warn("Missing columns detected in the DataFrame: {}".format(colname))
            _LOG.warn("Adding a noew column with value `{}` and type `{}`".format(coltype_and_default['default'], coltype_and_default['type']))
            name = colname.replace('.', '_')
            df = df.withColumn(name, F.lit(coltype_and_default['default']))
            tmp_d.append(F.col(name).cast(coltype_and_default['type']))

    cols_d = df.select(tmp_d).columns

    # flatten names
    cnames = tmp_i + tmp_d + tmp_b
    df = df.select(cnames)

    return df, cols_i, cols_d, cols_b

def load_ztf_index_cols():
    """ Load columns used for index tables (flattened and casted before).

    Returns
    ---------
    out: list of string
        List of (flattened) column names

    Examples
    --------
    >>> out = load_ztf_index_cols()
    >>> print(len(out))
    52
    """
    common = [
        'objectId', 'candid', 'publisher', 'rcid', 'chipsf', 'distnr',
        'ra', 'dec', 'jd', 'fid', 'nid', 'field', 'xpos', 'ypos', 'rb',
        'ssdistnr', 'ssmagnr', 'ssnamenr', 'jdstarthist', 'jdendhist', 'tooflag',
        'sgscore1', 'distpsnr1', 'neargaia', 'maggaia', 'nmtchps', 'diffmaglim',
        'magpsf', 'sigmapsf', 'magnr', 'sigmagnr', 'magzpsci', 'isdiffpos',
        'cdsxmatch',
        'roid',
        'mulens',
        'DR3Name',
        'Plx',
        'e_Plx',
        'gcvs',
        'vsx',
        'snn_snia_vs_nonia', 'snn_sn_vs_all', 'rf_snia_vs_nonia',
        'classtar', 'drb', 'ndethist', 'rf_kn_vs_nonkn', 'tracklet',
        'anomaly_score', 'x4lac', 'x3hsp'
    ]

    mangrove = [
        'mangrove_2MASS_name',
        'mangrove_HyperLEDA_name',
        'mangrove_ang_dist',
        'mangrove_lum_dist'
    ]

    t2 = [
        't2_AGN',
        't2_EB',
        't2_KN',
        't2_M-dwarf',
        't2_Mira',
        't2_RRL',
        't2_SLSN-I',
        't2_SNII',
        't2_SNIa',
        't2_SNIa-91bg',
        't2_SNIax',
        't2_SNIbc',
        't2_TDE',
        't2_mu-Lens-Single',
    ]

    return common + mangrove + t2

def load_ztf_crossmatch_cols():
    """ Load columns used for the crossmatch table (casted).

    Returns
    ---------
    out: list of string
        List of column names casted

    Examples
    --------
    >>> out = load_ztf_crossmatch_cols()
    >>> print(len(out))
    12
    """
    to_use = [
        'objectId',
        'candid'
        'magpsf',
        'sigmapsf',
        'jd',
        'jdstarthist',
        'cdsxmatch',
        'drb',
        'ra',
        'dec',
        'fid',
        'distnr',
    ]

    return to_use

def load_hbase_data(catalog: str, rowkey: str) -> DataFrame:
    """ Load table data from HBase into a Spark DataFrame

    The row(s) containing the different schemas are skipped (only data is loaded)

    Parameters
    ----------
    catalog: str
        Json string containing the HBase table. See
        `fink_utils.hbase` for more information.
    rowkey: str
        Name of the rowkey

    Returns
    ----------
    df: DataFrame
        Spark DataFrame with the table data
    """
    # Grab the running Spark Session,
    # otherwise create it.
    spark = SparkSession \
        .builder \
        .getOrCreate()

    df = spark.read.option("catalog", catalog)\
        .format("org.apache.hadoop.hbase.spark")\
        .option("hbase.spark.use.hbasecontext", False)\
        .option("hbase.spark.pushdown.columnfilter", True)\
        .load()\
        .filter(~F.col(rowkey).startswith('schema_'))

    return df

def write_catalog_on_disk(catalog, catalogname) -> None:
    """ Save HBase catalog in json format on disk

    Parameters
    ----------
    catalog: str
        Str-dictionary containing the HBase catalog constructed
        from `construct_hbase_catalog_from_flatten_schema`
    catalogname: str
        Name of the catalog on disk
    """
    with open(catalogname, 'w') as json_file:
        json.dump(catalog, json_file)

def push_to_hbase(df, table_name, rowkeyname, cf, nregion=50, catfolder='.') -> None:
    """ Push DataFrame data to HBase

    Parameters
    ----------
    df: Spark DataFrame
        Spark DataFrame
    table_name: str
        Name of the table in HBase
    rowkeyname: str
        Name of the rowkey in the table
    cf: dict
        Dictionnary containing column names with column family
    nregion: int, optional
        Number of region to create if the table is newly created. Default is 50.
    catfolder: str
        Folder to write catalogs (must exist). Default is current directory.
    """
    # construct the catalog
    hbcatalog_index = construct_hbase_catalog_from_flatten_schema(
        df.schema,
        table_name,
        rowkeyname=rowkeyname,
        cf=cf
    )

    # Push table
    df.write\
        .options(catalog=hbcatalog_index, newtable=nregion)\
        .format("org.apache.hadoop.hbase.spark")\
        .option("hbase.spark.use.hbasecontext", False)\
        .save()

    # write catalog for the table data
    file_name = table_name + '.json'
    write_catalog_on_disk(hbcatalog_index, os.path.join(catfolder, file_name))

    # Construct the schema row - inplace replacement
    schema_row_key_name = 'schema_version'
    df = df.withColumnRenamed(
        rowkeyname,
        schema_row_key_name
    )

    df_index_schema = construct_schema_row(
        df,
        rowkeyname=schema_row_key_name,
        version='schema_{}_{}'.format(fbvsn, fsvsn))

    # construct the hbase catalog for the schema
    hbcatalog_index_schema = construct_hbase_catalog_from_flatten_schema(
        df_index_schema.schema,
        table_name,
        rowkeyname=schema_row_key_name,
        cf=cf)

    # Push the data using the hbase connector
    df_index_schema.write\
        .options(catalog=hbcatalog_index_schema, newtable=nregion)\
        .format("org.apache.hadoop.hbase.spark")\
        .option("hbase.spark.use.hbasecontext", False)\
        .save()

    # write catalog for the schema row
    file_name = table_name + '_schema_row.json'
    write_catalog_on_disk(
        hbcatalog_index_schema, os.path.join(catfolder, file_name)
    )

def load_science_portal_column_names():
    """ Load names of the alert fields to use in the science portal.

    These column names should match DataFrame column names. Careful when
    you update it, as it will change the structure of the HBase table.

    The column names are sorted by column family names:
        - i: for column that identify the alert (original alert)
        - d: for column that further describe the alert (Fink added value)
        - b: for binary blob (FITS image)

    Returns
    --------
    cols_*: list of string
        List of DataFrame column names to use for the science portal

    Examples
    --------
    >>> cols_i, cols_d, cols_b = load_science_portal_column_names()
    >>> print(len(cols_d))
    35
    """
    # Column family i
    cols_i = [
        'objectId',
        'schemavsn',
        'publisher',
        'fink_broker_version',
        'fink_science_version',
        'candidate.*'
    ]

    # Column family d
    cols_d = [
        'cdsxmatch',
        'rf_snia_vs_nonia',
        'snn_snia_vs_nonia',
        'snn_sn_vs_all',
        'mulens',
        'roid',
        'nalerthist',
        'rf_kn_vs_nonkn',
        'tracklet',
        'DR3Name',
        'Plx',
        'e_Plx',
        'gcvs',
        'vsx',
        'x4lac',
        'x3hsp',
        'anomaly_score'
    ]

    # mangrove
    cols_d += [
        F.col('mangrove.{}'.format(i)).alias('mangrove_{}'.format(i)) for i in MANGROVE_COLS
    ]

    cols_d += [
        F.col('t2.{}'.format(i)).alias('t2_{}'.format(i)) for i in T2_COLS
    ]

    # cols_d += [
    #     F.col('lc_features_g.{}'.format(i)).alias('lc_features_g_{}'.format(i)) for i in FEATURES_COLS
    # ]

    # cols_d += [
    #     F.col('lc_features_r.{}'.format(i)).alias('lc_features_r_{}'.format(i)) for i in FEATURES_COLS
    # ]

    # Column family binary
    cols_b = [
        F.col('cutoutScience.stampData').alias('cutoutScience_stampData'),
        F.col('cutoutTemplate.stampData').alias('cutoutTemplate_stampData'),
        F.col('cutoutDifference.stampData').alias('cutoutDifference_stampData')
    ]

    return cols_i, cols_d, cols_b

def select_relevant_columns(df: DataFrame, cols: list, row_key_name: str, to_create=None) -> DataFrame:
    """ Select columns from `cols` that are actually in `df`.

    It would act as if `df.select(cols, skip_unknown_cols=True)` was possible. Note though
    that nested cols in `cols` will be flatten, and columns used in `to_create` have to be
    in this list of flatten names. Example, if my initial df has schema
    root
    |-- objectId: string (nullable = true)
    |-- candidate: struct (nullable = true)
    |    |-- jd: double (nullable = true)

    then `to_create` can be `F.col('objectId') + F.col('jd')` but
    not `F.col('objectId') + F.col('candidate.jd')`

    Parameters
    ----------
    df: DataFrame
        Input Spark DataFrame
    cols: list
        Column names to select
    row_key_name: str
        Row key name
    to_create: list
        Extra columns to create from others, and to include in the `select`.
        Example: df.select(['a', 'b', F.col('a') + F.col('c')])

    Returns:
    df: DataFrame

    Examples
    ----------
    >>> import pyspark.sql.functions as F
    >>> df = spark.createDataFrame([{'a': 1, 'b': 2, 'c': 3}])

    >>> select_relevant_columns(df, ['a'], '')
    DataFrame[a: bigint]

    >>> select_relevant_columns(df, ['a', 'b', 'c'], '')
    DataFrame[a: bigint, b: bigint, c: bigint]

    >>> select_relevant_columns(df, ['a', 'd'], '')
    DataFrame[a: bigint]

    >>> select_relevant_columns(df, ['a', 'b'], 'c', to_create=[F.col('a') + F.col('b')])
    DataFrame[a: bigint, b: bigint, c: bigint, (a + b): bigint]
    """
    # Add the row key to the list of columns to extract
    all_cols = cols + [row_key_name]

    if (to_create is not None) and (type(to_create) == list):
        for extra_col in to_create:
            all_cols += [extra_col]

    cnames = []
    missing_cols = []
    for col_ in all_cols:
        # Dumb but simple
        try:
            df.select(col_)
            cnames.append(col_)
        except AnalysisException:
            missing_cols.append(col_)

    # flatten names
    df = df.select(cnames)

    _LOG.info("Missing columns detected in the DataFrame: {}".format(missing_cols))

    return df

def assign_column_family_names(df, cols_i, cols_d, cols_b):
    """ Assign a column family name to each column qualifier.

    There are currently 3 column families:
        - i: for column that identify the alert (original alert)
        - d: for column that further describe the alert (Fink added value)
        - b: for binary types. It currently contains:
            - binary gzipped FITS image

    The split is done in `load_science_portal_column_names`.

    Parameters
    ----------
    df: DataFrame
        Input DataFrame containing alert data from the raw science DB (parquet).
        See `load_parquet_files` for more information.
    cols_*: list of string
        List of DataFrame column names to use for the science portal.

    Returns
    ---------
    cf: dict
        Dictionary with keys being column names (also called
        column qualifiers), and the corresponding column family.

    """
    cf = {i: 'i' for i in df.select(cols_i).columns}
    cf.update({i: 'd' for i in df.select(cols_d).columns})
    cf.update({i: 'b' for i in df.select(cols_b).columns})

    return cf

def construct_hbase_catalog_from_flatten_schema(
        schema: dict, catalogname: str, rowkeyname: str, cf: dict) -> str:
    """ Convert a flatten DataFrame schema into a HBase catalog.

    From
    {'name': 'schemavsn', 'type': 'string', 'nullable': True, 'metadata': {}}

    To
    'schemavsn': {'cf': 'i', 'col': 'schemavsn', 'type': 'string'},

    Parameters
    ----------
    schema : dict
        Schema of the flatten DataFrame.
    catalogname : str
        Name of the HBase catalog.
    rowkeyname : str
        Name of the rowkey in the HBase catalog.
    cf: dict
        Dictionary with keys being column names (also called
        column qualifiers), and the corresponding column family.
        See `assign_column_family_names`.

    Returns
    ----------
    catalog : str
        Catalog for HBase.

    Examples
    --------
    # Read alert from the raw database
    >>> df = spark.read.format("parquet").load(ztf_alert_sample_scidatabase)

    >>> cols_i, cols_d, cols_b = load_science_portal_column_names()

    >>> cf = assign_column_family_names(df, cols_i, [], [])

    # Flatten the DataFrame
    >>> df_flat = df.select(cols_i)

    Attach the row key
    >>> df_rk = add_row_key(df_flat, 'objectId_jd', cols=['objectId', 'jd'])

    >>> catalog = construct_hbase_catalog_from_flatten_schema(
    ...     df_rk.schema, "mycatalogname", 'objectId_jd', cf)
    """
    schema_columns = schema.jsonValue()["fields"]

    catalog = ''.join("""
    {{
        'table': {{
            'namespace': 'default',
            'name': '{}'
        }},
        'rowkey': '{}',
        'columns': {{
    """).format(catalogname, rowkeyname)

    sep = ","
    for column in schema_columns:
        # Last entry should not have comma (malformed json)
        # if schema_columns.index(column) != len(schema_columns) - 1:
        #     sep = ","
        # else:
        #     sep = ""

        # Deal with array
        if type(column["type"]) == dict:
            # column["type"]["type"]
            column["type"] = "string"

        if column["type"] == 'timestamp':
            # column["type"]["type"]
            column["type"] = "string"

        if column["name"] == rowkeyname:
            catalog += """
            '{}': {{'cf': 'rowkey', 'col': '{}', 'type': '{}'}}{}
            """.format(column["name"], column["name"], column["type"], sep)
        else:
            catalog += """
            '{}': {{'cf': '{}', 'col': '{}', 'type': '{}'}}{}
            """.format(
                column["name"],
                cf[column["name"]],
                column["name"],
                column["type"],
                sep
            )

    # Push an empty column family 'a' for later annotations
    catalog += "'annotation': {'cf': 'a', 'col': '', 'type': 'string'}"
    catalog += """
        }
    }
    """

    return catalog.replace("\'", "\"")

def construct_schema_row(df, rowkeyname, version):
    """ Construct a DataFrame whose columns are those of the
    original ones, and one row containing schema types

    Parameters
    ----------
    df: Spark DataFrame
        Input Spark DataFrame. Need to be flattened.
    rowkeyname: string
        Name of the HBase row key (column name)
    version: string
        Version of the HBase table (row value for the rowkey column).

    Returns
    ---------
    df_schema: Spark DataFrame
        Spark DataFrame with one row (the types of its column). Only the row
        key is the version of the HBase table.

    Examples
    --------
    # Read alert from the raw database
    >>> df = spark.read.format("parquet").load(ztf_alert_sample_scidatabase)

    # inplace replacement
    >>> df = df.select(['objectId', 'candidate.jd', 'candidate.candid', F.col('cutoutScience.stampData').alias('cutoutScience_stampData')])
    >>> df = df.withColumn('schema_version', F.lit(''))
    >>> df = construct_schema_row(df, rowkeyname='schema_version', version='schema_v0')
    >>> df.show()
    +--------+------+------+-----------------------+--------------+
    |objectId|    jd|candid|cutoutScience_stampData|schema_version|
    +--------+------+------+-----------------------+--------------+
    |  string|double|  long|             fits/image|     schema_v0|
    +--------+------+------+-----------------------+--------------+
    <BLANKLINE>
    """
    # Grab the running Spark Session,
    # otherwise create it.
    spark = SparkSession \
        .builder \
        .getOrCreate()

    # Original df columns, but values are types.
    data = np.array([(c.jsonValue()['type']) for c in df.schema], dtype='<U75')

    # binary types are too vague, so assign manually a description
    names = np.array([(c.jsonValue()['name']) for c in df.schema])
    mask = np.array(['cutout' in i for i in names])
    data[mask] = 'fits/image'

    index = np.where(np.array(df.columns) == rowkeyname)[0][0]
    data[index] = version

    # Create the DataFrame
    df_schema = spark.createDataFrame([data.tolist()], df.columns)

    return df_schema

def add_row_key(df, row_key_name, cols=[]):
    """ Create and attach the row key to a DataFrame

    This should be typically called before `select_relevant_columns`.

    Parameters
    ----------
    df: DataFrame
        Spark DataFrame
    row_key_name: str
        Row key name (typically columns seprated by _)
    cols: list
        List of columns to concatenate

    Returns
    ----------
    out: DataFrame
        Original Spark DataFrame with a new column
    """
    row_key_col = F.concat_ws('_', *cols).alias(row_key_name)
    df = df.withColumn(row_key_name, row_key_col)

    return df

def push_full_df_to_hbase(df, row_key_name, table_name, catalog_name):
    """ Push data stored in a Spark DataFrame into HBase

    It assumes the main ZTF table schema

    Parameters
    ----------
    df: Spark DataFrame
        Spark DataFrame (full alert schema)
    row_key_name: str
        Name of the rowkey in the table. Should be a column name
        or a combination of column separated by _ (e.g. jd_objectId).
    table_name: str
        HBase table name. If it does not exist, it will
        be created.
    catalog_name: str
        Name for the JSON catalog (saved locally for inspection)
    """
    # Check all columns exist, fill if necessary, and cast data
    df_flat, cols_i, cols_d, cols_b = bring_to_current_schema(df)

    # Assign each column to a specific column family
    # This is independent from the final structure
    cf = assign_column_family_names(df_flat, cols_i, cols_d, cols_b)

    # Restrict the input DataFrame to the subset of wanted columns.
    all_cols = cols_i + cols_d + cols_b

    df_flat = add_row_key(
        df_flat,
        row_key_name=row_key_name,
        cols=row_key_name.split('_')
    )

    # Flatten columns
    df_flat = select_relevant_columns(
        df_flat,
        row_key_name=row_key_name,
        cols=all_cols,
    )

    push_to_hbase(
        df=df_flat,
        table_name=table_name,
        rowkeyname=row_key_name,
        cf=cf,
        catfolder=catalog_name
    )


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    globs = globals()
    root = os.environ['FINK_HOME']
    globs["ztf_alert_sample"] = os.path.join(
        root, "fink-alert-schemas/ztf/template_schema_ZTF_3p3.avro")

    globs["ztf_alert_sample_scidatabase"] = os.path.join(
        root, "online/science")

    # Run the Spark test suite
    spark_unit_tests(globs, withstreaming=False)
