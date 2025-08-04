# Copyright 2019-2025 AstroLab Software
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
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException

from fink_broker import __version__ as fbvsn
from fink_science import __version__ as fsvsn

import logging
import json
import os
import numpy as np

from fink_broker.common.tester import spark_unit_tests

_LOG = logging.getLogger(__name__)


def load_hbase_data(catalog: str, rowkey: str) -> DataFrame:
    """Load table data from HBase into a Spark DataFrame

    The row(s) containing the different schemas are skipped (only data is loaded)

    Parameters
    ----------
    catalog: str
        Json string containing the HBase table. See
        `fink_utils.hbase` for more information.
    rowkey: str
        Name of the rowkey

    Returns
    -------
    df: DataFrame
        Spark DataFrame with the table data
    """
    # Grab the running Spark Session,
    # otherwise create it.
    spark = SparkSession.builder.getOrCreate()

    df = (
        spark.read.option("catalog", catalog)
        .format("org.apache.hadoop.hbase.spark")
        .option("hbase.spark.use.hbasecontext", False)
        .option("hbase.spark.pushdown.columnfilter", True)
        .load()
        .filter(~F.col(rowkey).startswith("schema_"))
    )

    return df


def select_relevant_columns(
    df: DataFrame, cols: list, row_key_name: str, to_create=None
) -> DataFrame:
    """Select columns from `cols` that are actually in `df`.

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

    Returns
    -------
    df: DataFrame

    Examples
    --------
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

    if (to_create is not None) and isinstance(to_create, list):
        for extra_col in to_create:
            all_cols += [extra_col]

    cnames = []
    missing_cols = []
    for col_ in all_cols:
        # Dumb but simple
        try:  # noqa: PERF203
            df.select(col_)
            cnames.append(col_)
        except AnalysisException:  # noqa: PERF203
            missing_cols.append(col_)

    # flatten names
    df = df.select(cnames)

    _LOG.info("Missing columns detected in the DataFrame: {}".format(missing_cols))

    return df


def assign_column_family_names(df, cols_i, cols_d):
    """Assign a column family name to each column qualifier.

    There are currently 2 column families:
        - i: for column that identify the alert (original alert)
        - d: for column that further describe the alert (Fink added value)

    The split is done in `bring_to_current_schema`.

    Parameters
    ----------
    df: DataFrame
        Input DataFrame containing alert data from the raw science DB (parquet).
        See `load_parquet_files` for more information.
    cols_*: list of string
        List of DataFrame column names to use for the science portal.

    Returns
    -------
    cf: dict
        Dictionary with keys being column names (also called
        column qualifiers), and the corresponding column family.

    """
    cf = {i: "i" for i in df.select(["`{}`".format(k) for k in cols_i]).columns}
    cf.update({i: "d" for i in df.select(["`{}`".format(k) for k in cols_d]).columns})

    return cf


def construct_hbase_catalog_from_flatten_schema(
    schema: dict, catalogname: str, rowkeyname: str, cf: dict
) -> str:
    """Convert a flatten DataFrame schema into a HBase catalog.

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
    -------
    catalog : str
        Catalog for HBase.

    Examples
    --------
    # Read alert from the raw database
    >>> df = spark.read.format("parquet").load(ztf_alert_sample_scidatabase)

    >>> df_flat, cols_i, cols_d = bring_to_current_schema(df)

    >>> cf = assign_column_family_names(df_flat, cols_i, cols_d)

    Attach the row key
    >>> df_rk = add_row_key(df_flat, 'objectId_jd', cols=['objectId', 'jd'])

    >>> catalog = construct_hbase_catalog_from_flatten_schema(
    ...     df_rk.schema, "mycatalogname", 'objectId_jd', cf)
    """
    schema_columns = schema.jsonValue()["fields"]

    catalog = "".join("""
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
        if isinstance(column["type"], dict):
            # column["type"]["type"]
            column["type"] = "string"

        if column["type"] == "timestamp":
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
                column["name"], cf[column["name"]], column["name"], column["type"], sep
            )

    # Push an empty column family 'a' for later annotations
    catalog += "'annotation': {'cf': 'a', 'col': '', 'type': 'string'}"
    catalog += """
        }
    }
    """

    return catalog.replace("'", '"')


def construct_schema_row(df, rowkeyname, version):
    """Construct a DataFrame whose columns are those of the original ones, and one row containing schema types

    Parameters
    ----------
    df: Spark DataFrame
        Input Spark DataFrame. Need to be flattened.
    rowkeyname: string
        Name of the HBase row key (column name)
    version: string
        Version of the HBase table (row value for the rowkey column).

    Returns
    -------
    df_schema: Spark DataFrame
        Spark DataFrame with one row (the types of its column). Only the row
        key is the version of the HBase table.

    Examples
    --------
    # Read alert from the raw database
    >>> df = spark.read.format("parquet").load(ztf_alert_sample_scidatabase)

    # inplace replacement
    >>> df = df.select(['objectId', 'candidate.jd', 'candidate.candid'])
    >>> df = df.withColumn('schema_version', F.lit(''))
    >>> df = construct_schema_row(df, rowkeyname='schema_version', version='schema_v0')
    >>> df.show()
    +--------+------+------+--------------+
    |objectId|    jd|candid|schema_version|
    +--------+------+------+--------------+
    |  string|double|  long|     schema_v0|
    +--------+------+------+--------------+
    <BLANKLINE>
    """
    # Grab the running Spark Session,
    # otherwise create it.
    spark = SparkSession.builder.getOrCreate()

    # Original df columns, but values are types.
    data = np.array([(c.jsonValue()["type"]) for c in df.schema], dtype="<U75")

    index = np.where(np.array(df.columns) == rowkeyname)[0][0]
    data[index] = version

    # Create the DataFrame
    df_schema = spark.createDataFrame([data.tolist()], df.columns)

    return df_schema


def add_row_key(df, row_key_name, cols=None):
    """Create and attach the row key to a DataFrame

    This should be typically called before `select_relevant_columns`.

    Parameters
    ----------
    df: DataFrame
        Spark DataFrame
    row_key_name: str
        Row key name (typically columns separated by _)
    cols: list
        List of columns to concatenate (typically split row_key_name)

    Returns
    -------
    out: DataFrame
        Original Spark DataFrame with a new column

    Examples
    --------
    # Read alert from the raw database
    >>> df = spark.read.format("parquet").load(ztf_alert_sample_scidatabase)

    # Flatten columns
    >>> df = df.select(["objectId", "candidate.jd", "candidate.ra"])

    >>> df2 = add_row_key(df, None, None)
    >>> extra_cols = [col for col in df2.columns if col not in df.columns]
    >>> assert len(extra_cols) == 0, "Found {}".format(extra_cols)

    >>> rowkey = "objectId"
    >>> df2 = add_row_key(df, rowkey, rowkey.split("_"))
    >>> extra_cols = [col for col in df2.columns if col not in df.columns]
    >>> assert len(extra_cols) == 0, "Found {}".format(extra_cols)

    >>> rowkey = "objectId_jd"
    >>> df2 = add_row_key(df, rowkey, rowkey.split("_"))
    >>> extra_cols = [col for col in df2.columns if col not in df.columns]
    >>> assert extra_cols == [rowkey], "Found {}".format(extra_cols)

    >>> rowkey = "objectId_objectId"
    >>> df2 = add_row_key(df, rowkey, rowkey.split("_")) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    AssertionError: You have duplicated fields in your columns definition: ['objectId', 'objectId']

    >>> rowkey = "objectId_toto"
    >>> df2 = add_row_key(df, rowkey, rowkey.split("_")) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    AssertionError: Cannot build the rowkey: toto is not in DataFrame Columns ['objectId', 'jd', 'ra']
    """
    if not isinstance(cols, list):
        # should never happen in practice
        return df

    if len(cols) == 1:
        # single field rowkey
        return df

    # check all fields exist
    msg = "Cannot build the rowkey: {} is not in DataFrame Columns {}"
    for col in cols:
        assert col in df.columns, msg.format(col, df.columns)

    # check there is no duplicates
    msg = "You have duplicated fields in your columns definition: {}".format(cols)
    assert len(np.unique(cols)) == len(cols), msg

    row_key_col = F.concat_ws("_", *cols).alias(row_key_name)
    df = df.withColumn(row_key_name, row_key_col)

    return df


def write_catalog_on_disk(catalog, catfolder, file_name) -> None:
    """Save HBase catalog in json format on disk

    Parameters
    ----------
    catalog: str
        Str-dictionary containing the HBase catalog constructed
        from `construct_hbase_catalog_from_flatten_schema`
    catalogname: str
        Name of the catalog on disk
    """
    if not os.path.isdir(catfolder):
        os.makedirs(catfolder, exist_ok=True)
    catalogname = os.path.join(catfolder, file_name)
    with open(catalogname, "w") as json_file:
        json.dump(catalog, json_file)


def push_to_hbase(df, table_name, rowkeyname, cf, nregion=50, catfolder=".") -> None:
    """Push DataFrame data to HBase

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
        df.schema, table_name, rowkeyname=rowkeyname, cf=cf
    )

    # Push table
    df.write.options(catalog=hbcatalog_index, newtable=nregion).format(
        "org.apache.hadoop.hbase.spark"
    ).option("hbase.spark.use.hbasecontext", False).save()

    # write catalog for the table data
    file_name = table_name + ".json"
    write_catalog_on_disk(hbcatalog_index, catfolder, file_name)

    # Construct the schema row - inplace replacement
    schema_row_key_name = "schema_version"
    df = df.withColumnRenamed(rowkeyname, schema_row_key_name)

    df_index_schema = construct_schema_row(
        df, rowkeyname=schema_row_key_name, version="schema_{}_{}".format(fbvsn, fsvsn)
    )

    # construct the hbase catalog for the schema
    hbcatalog_index_schema = construct_hbase_catalog_from_flatten_schema(
        df_index_schema.schema, table_name, rowkeyname=schema_row_key_name, cf=cf
    )

    # Push the data using the hbase connector
    df_index_schema.write.options(
        catalog=hbcatalog_index_schema, newtable=nregion
    ).format("org.apache.hadoop.hbase.spark").option(
        "hbase.spark.use.hbasecontext", False
    ).save()

    # write catalog for the schema row
    file_name = table_name + "_schema_row.json"
    write_catalog_on_disk(hbcatalog_index_schema, catfolder, file_name)


def flatten_dataframe(df, root_level, section, fink_cols, fink_nested_cols):
    """Flatten DataFrame columns of a nested Spark DF for HBase ingestion

    Notes
    -----
    Check also all Fink columns exist, fill if necessary, and cast all columns.

    Parameters
    ----------
    df: DataFrame
        Spark DataFrame with raw alert data
    root_level: dict
        Dictionary with root level columns
    section: dict
        Dictionary with nested level columns.
        For ZTF, this will be `candidates`.
        For Rubin, this will be `diaSource` or `diaObject`
    fink_cols: dict
        Dictionary with Fink root level columns
    fink_nested_cols: dict
        Dictionary with Fink nested columns

    Returns
    -------
    df: DataFrame
        Spark DataFrame with HBase data structure
    col_i: list
        List of columns for i column family
    col_d: list
        List of columns for d column family
    cf: dict
        Dictionary with keys being column names (also called
        column qualifiers), and the corresponding column family.

    """
    tmp_i = []
    tmp_d = []

    # assuming no missing columns
    for colname, coltype in root_level.items():
        tmp_i.append(F.col(colname).cast(coltype))

    # assuming no missing columns
    for colname, coltype in section.items():
        tmp_i.append(F.col(colname).cast(coltype).alias(colname.split(".")[-1]))

    cols_i = df.select(tmp_i).columns

    # check all columns exist, otherwise create it
    for colname, coltype_and_default in fink_cols.items():
        try:
            # ony here to check if the column exists
            df.select(colname)
        except AnalysisException:
            _LOG.warn("Missing columns detected in the DataFrame: {}".format(colname))
            _LOG.warn(
                "Adding a new column with value `{}` and type `{}`".format(
                    coltype_and_default["default"], coltype_and_default["type"]
                )
            )
            df = df.withColumn(colname, F.lit(coltype_and_default["default"]))
        tmp_d.append(F.col(colname).cast(coltype_and_default["type"]))

    # check all columns exist, otherwise create it
    for colname, coltype_and_default in fink_nested_cols.items():
        try:  # noqa: PERF203
            # ony here to check if the column exists
            df.select(colname)

            # rename root.level into root_level
            name = (
                F.col(colname)
                .alias(colname.replace(".", "_"))
                .cast(coltype_and_default["type"])
            )
            tmp_d.append(name)
        except AnalysisException:  # noqa: PERF203
            _LOG.warn("Missing columns detected in the DataFrame: {}".format(colname))
            _LOG.warn(
                "Adding a new column with value `{}` and type `{}`".format(
                    coltype_and_default["default"], coltype_and_default["type"]
                )
            )
            name = colname.replace(".", "_")
            df = df.withColumn(name, F.lit(coltype_and_default["default"]))
            tmp_d.append(F.col(name).cast(coltype_and_default["type"]))

    cols_d = df.select(tmp_d).columns

    # flatten names
    cnames = tmp_i + tmp_d
    df = df.select(cnames)

    cf = assign_column_family_names(df, cols_i, cols_d)

    return df, cols_i, cols_d, cf


def salt_from_diaobjectid(df, npartitions):
    """Add simple salt column based on the last digits of diaObjectId

    Notes
    -----
    The key will be the last N digits of diaObject.diaObjectId.
    N = int(log10(npartitions))
    The key will be padded with 0 for safety.

    Parameters
    ----------
    df: Spark DataFrame
        Input Spark dataframe
    npartitions: int
        Number of partitions in the HBase table

    Returns
    -------
    df: Spark DataFrame
        Input df with a new column `salt` containing
        the partitioning key
    """
    # Key prefix will be the last N digits
    # This must match the number of partitions in the table
    ndigits = int(np.log10(npartitions))
    df = df.withColumn(
        "salt",
        F.lpad(F.substring("diaObject.diaObjectId", -ndigits, ndigits), ndigits, "0"),
    )

    return df


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    globs = globals()
    root = os.environ["FINK_HOME"]
    globs["ztf_alert_sample"] = os.path.join(
        root, "fink-alert-schemas/ztf/template_schema_ZTF_3p3.avro"
    )

    globs["ztf_alert_sample_scidatabase"] = os.path.join(
        root, "online/science/20200101"
    )

    # Run the Spark test suite
    spark_unit_tests(globs, withstreaming=False)
