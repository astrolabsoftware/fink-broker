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
from pyspark.sql import Window
from pyspark.sql.utils import AnalysisException

from fink_broker.common.hbase_utils import add_row_key
from fink_broker.common.hbase_utils import push_to_hbase
from fink_broker.common.hbase_utils import salt_from_last_digits
from fink_broker.common.spark_utils import load_parquet_files

from fink_science.ztf.xmatch.utils import MANGROVE_COLS  # FIXME: common

from fink_broker.common.tester import spark_unit_tests

import pandas as pd
import os
import logging

_LOG = logging.getLogger(__name__)


def load_fink_cols():
    """Fink-derived columns used in HBase tables with type.

    Notes
    -----
    Keys are column names (flattened). Values are data type and default.

    Returns
    -------
    fink_source_cols: dictionary
        Fink added values for each source.
    fink_object_cols: dictionary
        Fink added values for each object.


    Examples
    --------
    >>> fink_source_cols, fink_object_cols = load_fink_cols()
    >>> print(len(fink_source_cols))
    19
    """
    fink_source_cols = {
        # Crossmatch
        "DR3Name": {
            "type": "string",
            "default": "Unknown",
        },  # FIXME: prefix xmatch by catalog
        "Plx": {"type": "float", "default": 0.0},  # FIXME: prefix xmatch by catalog
        "cdsxmatch": {
            "type": "string",
            "default": "Unknown",
        },  # FIXME: prefix xmatch by catalog
        "e_Plx": {"type": "float", "default": 0.0},  # FIXME: prefix xmatch by catalog
        "gcvs": {
            "type": "string",
            "default": "Unknown",
        },  # FIXME: prefix xmatch by catalog
        "vsx": {
            "type": "string",
            "default": "Unknown",
        },  # FIXME: prefix xmatch by catalog
        "x3hsp": {
            "type": "string",
            "default": "Unknown",
        },  # FIXME: prefix xmatch by catalog
        "x4lac": {
            "type": "string",
            "default": "Unknown",
        },  # FIXME: prefix xmatch by catalog
        "spicy_id": {"type": "int", "default": -1},  # FIXME: prefix xmatch by catalog
        "spicy_class": {
            "type": "string",
            "default": "Unknown",
        },  # FIXME: prefix xmatch by catalog
        "tns": {"type": "string", "default": ""},  # FIXME: prefix xmatch by catalog
        "gaiaVarFlag": {"type": "int", "default": 0},  # FIXME: prefix xmatch by catalog
        "gaiaClass": {
            "type": "string",
            "default": "Unknown",
        },  # FIXME: prefix xmatch by catalog
        # Machine learning
        "rf_snia_vs_nonia": {"type": "double", "default": 0.0},  # FIXME: float?
        "rf_slsn_vs_nonslsn": {"type": "double", "default": 0.0},  # FIXME: float?
        "cats_broad_class": {
            "type": "int",
            "default": 11,
        },  # FIXME: what should the default?
        "snn_sn_vs_others": {"type": "double", "default": 0.0},  # FIXME: float?
        # Other
        "roid": {"type": "int", "default": 0},
        "fink_source_class": {"type": "string", "default": "Unknown"},
    }

    for col_ in MANGROVE_COLS:
        name = "mangrove.{}".format(col_)
        fink_source_cols.update({name: {"type": "string", "default": "None"}})

    # FIXME: is_cataloged, classification, etc.
    fink_object_cols = {}

    return fink_source_cols, fink_object_cols


def select_type(atype, name):
    """Choose the non-null type

    Parameters
    ----------
    atype: list or str
        Type of the field. Can be str (e.g. 'float'),
        or list (e.g. ['null', 'float'])
    name: str
        Name of the field. Only used if an error is raised.

    Returns
    -------
    out: str
        Type as string

    Examples
    --------
    >>> atype = ['null', 'float']
    >>> select_type(atype, "")
    'float'
    """
    if isinstance(atype, list):
        # of type ["null", "something else"]
        atype.remove("null")
        return atype[0]
    elif isinstance(atype, str) and atype != "null":
        return atype
    else:
        raise ValueError("Type {} for field {} is not supported".format(atype, name))


def extract_avsc_schema(name, major_version, minor_version):
    """Convert avsc into useful dictionary

    Parameters
    ----------
    name: str
        Name of the avsc to extract. In the form:
        lsst.v{major_version}_{minor_version}.{name}.avsc
    major_version: int
        Schema major version
    minor_version: int
        Schema minor version

    Returns
    -------
    dic: dict
        Dictionary whose keys are field name, and
        values are types and default values.

    Examples
    --------
    >>> schema = extract_avsc_schema("diaSource", 9, 0)
    >>> len(schema)
    98

    >>> schema["psfFlux"]
    {'type': 'float', 'default': None}

    >>> schema = extract_avsc_schema("diaObject", 9, 0)
    >>> len(schema)
    82
    """
    _LOG.info("{} schema version {}.{}".format(name, major_version, minor_version))

    baseurl = "https://raw.githubusercontent.com/lsst/alert_packet/refs/heads/main/python/lsst/alert/packet/schema"
    pdf = pd.read_json(
        os.path.join(
            baseurl,
            "{}/{}".format(major_version, minor_version),
            "lsst.v{}_{}.{}.avsc".format(major_version, minor_version, name),
        )
    )

    dic = {}

    fields = (
        pdf["fields"]
        .apply(
            lambda x: {
                x.get("name"): {
                    "type": select_type(x.get("type"), x.get("name")),
                    "default": x.get("default", None),
                }
            }
        )
        .to_list()
    )

    [dic.update(el) for el in fields]

    return dic


def load_rubin_root_level(include_salt=True):
    """Root level fields for Rubin alerts

    Parameters
    ----------
    include_salt: bool
        Include salting columns. Default is True.

    Returns
    -------
    out: dict
        Dictionary with root level fields

    Examples
    --------
    >>> root_level = load_rubin_root_level()
    >>> len(load_rubin_root_level())
    6

    >>> assert "fink_broker_version" in root_level, root_level
    """
    root_level = {
        "fink_broker_version": "string",
        "fink_science_version": "string",
        "lsst_schema_version": "string",
        "observation_reason": "string",
        "target_name": "string",
    }

    if include_salt:
        # This is not included in the parquet files.
        # Added at the level of HBase ingestion.
        root_level.update({"salt": "string"})
    return root_level


def load_all_rubin_cols(major_version, minor_version, include_salt=True):
    """Fink/LSST columns used in HBase tables with type.

    Parameters
    ----------
    major_version: int
        Schema major version
    minor_version: int
        Schema minor version
    include_salt: bool
        Include salting columns. Default is True.

    Returns
    -------
    out: dictionary
        Keys are column names (flattened). Values are data type.

    Examples
    --------
    >>> root_level, diaobject, mpcorb, diasource, sssource, fink_source_cols, fink_object_cols = load_all_rubin_cols(9, 0)
    >>> out = {
    ...     **root_level.values(), **diaobject.values(),
    ...     **mpcorb.values(), **diasource.values(),
    ...     **sssource.values(), **fink_source_cols.values(),
    ...     **fink_object_cols.values()}
    >>> expected = 6 + 82 + 12 + 98 + 24 + 19 + 4
    >>> assert len(out) == expected, (len(out), expected)
    """
    fink_source_cols, fink_object_cols = load_fink_cols()

    root_level = load_rubin_root_level(include_salt=include_salt)

    diasource_schema = extract_avsc_schema("diaSource", major_version, minor_version)
    diasource = {"diaSource." + k: v["type"] for k, v in diasource_schema.items()}

    sssource_schema = extract_avsc_schema("ssSource", major_version, minor_version)
    sssource = {"diaSource." + k: v["type"] for k, v in sssource_schema.items()}

    diaobject_schema = extract_avsc_schema("diaObject", major_version, minor_version)
    diaobject = {"diaObject." + k: v["type"] for k, v in diaobject_schema.items()}

    mpcorb_schema = extract_avsc_schema("MPCORB", major_version, minor_version)
    mpcorb = {"MPCORB." + k: v["type"] for k, v in mpcorb_schema.items()}

    # FIXME: add ssObject {"sso": ssobject}

    return (
        {"r": root_level},
        {"o": diaobject},
        {"s": diasource},
        {"sso": mpcorb},
        {"sss": sssource},
        {"fs": fink_source_cols},
        {"fo": fink_object_cols},
    )


def load_rubin_index_cols():
    """Load columns used for index tables (flattened and casted before).

    Returns
    -------
    out: list of string
        List of (flattened) column names

    Examples
    --------
    >>> out = load_rubin_index_cols()
    >>> print(len(out))
    13
    """
    # All columns from root_level
    common = list(load_rubin_root_level().keys())

    # Some columns from diaObject
    # FIXME: anything regarding flux, time, band, reliability?
    # FIXME: anything regarding closest objects?
    common += [
        "diaObjectId",
        "ra",
        "dec",
        "nDiaSources",
        "firstDiaSourceMjdTai",
        "lastDiaSourceMjdTai",
    ]

    # Add only classfication from Fink
    common += ["finkclass"]

    return common


def cast_and_rename_field(colname, coltype, nested):
    """ """

    # rename root.level into root_level
    # name = (
    #     F.col(colname)
    #     .alias(colname.replace(".", "_"))
    #     .cast(coltype)
    # )

    if nested:
        # Assume section.real_colname
        return F.col(colname).cast(coltype).alias(colname.split(".")[-1])
    else:
        return F.col(colname).cast(coltype)


def flatten_dataframe(df, sections):
    """Flatten DataFrame columns of a nested Spark DF for HBase ingestion

    Notes
    -----
    Check also all Fink columns exist, fill if necessary, and cast all columns.

    Notes
    -----
    This is the LSST version, slightly different from the ZTF version

    Parameters
    ----------
    df: DataFrame
        Spark DataFrame with raw alert data
    sections: dict of dict
        Dictionary with nested level columns: {cf1: schema1, cf2, schema2, ...}
        with `schemaX` given by `load_all_rubin_cols`.

    Returns
    -------
    df: DataFrame
        Spark DataFrame with HBase data structure
    cols: list
        List of selected columns
    cf: dict
        Dictionary with keys being column family names and
        corresponding columns as values.
    """
    cols = []
    cf = {}

    for section in sections:
        cf_name = section[0]
        schema = section[1]

        cols_ = {}
        for colname, coltype_ in schema.items():
            if isinstance(coltype_, dict):
                # type & default (fink)
                coltype = coltype_["type"]
                default = coltype_["default"]
            else:
                # just type (lsst)
                coltype = coltype_
                default = None

            if "." in colname:
                nested = True
            else:
                nested = False

            try:
                # only here to check if the column exists
                df.select(colname)

                cols_.append(cast_and_rename_field(colname, coltype, nested))
            except AnalysisException:  # noqa: PERF203
                _LOG.warn(
                    "Missing columns detected in the DataFrame: {}".format(colname)
                )
                if default is not None:
                    _LOG.warn(
                        "Adding a new column with value `{}` and type `{}`".format(
                            default, coltype
                        )
                    )
                    df = df.withColumn(colname, F.lit(default))
                    cols_.append(cast_and_rename_field(colname, coltype, nested))
                else:
                    _LOG.warn(
                        "No default value was provided -- skipping the ingestion of the {} field".format(
                            colname
                        )
                    )

        # Assign cf ID to columns
        cf.update({
            i: cf_name for i in df.select(["`{}`".format(k) for k in cols_]).columns
        })

        # Update total columns
        cols += cols_

    # flatten all names
    df = df.select(cols)

    return df, cols, cf


def ingest_source_data(
    kind,
    paths,
    table_name,
    row_key_name,
    catfolder,
    major_version,
    minor_version,
    nfiles=100,
    npartitions=1000,
):
    """Push data to HBase by batch of parquet files

    Notes
    -----
    The row key is salted using the last 3 digits
    of diaObject.diaObjectId

    Parameters
    ----------
    kind: str
        static or sso alerts.
    paths: list
        List of paths to parquet files on HDFS
    table_name: str
        HBase table name, in the form `rubin.<suffix>`.
        Must exist in the cluster.
    row_key_name: str
        Name of the rowkey in the table. Should be a column name
        or a combination of column separated by _ (e.g. jd_objectId).
    catfolder: str
        Folder to save catalog (saved locally for inspection)
    major_version: int
        LSST alert schema major version (e.g. 9)
    minor_version: int
        LSST alert schema minor version (e.g. 0)
    nfiles: int
        Number of parquet files to ingest at once
    npartitions: int
        Number of HBase partitions in the table.

    Returns
    -------
    out: int
        Number of alerts ingested
    """
    if kind == "static":
        section = "diaObject"
        field = "diaObjectId"
        # Name of the rowkey in the table. Should be a column name
        # or a combination of column separated by _ (e.g. jd_objectId).
        row_key_name = "salt_diaObjectId_midpointMjdTai"
        table_name = "rubin.diaSource_static"
    elif kind == "sso":
        # FIXME: add ssObject when it will be available
        section = "MPCORB"
        field = "ssObjectId"
        # Name of the rowkey in the table. Should be a column name
        # or a combination of column separated by _ (e.g. jd_objectId).
        row_key_name = "salt_ssObjectId_midpointMjdTai"
        table_name = "rubin.diaSource_sso"

    nloops = int(len(paths) / nfiles) + 1
    n_alerts = 0
    for index in range(0, len(paths), nfiles):
        _LOG.info("Loop {}/{}".format(index + 1, nloops))
        df = load_parquet_files(paths[index : index + nfiles])

        # Keep only rows with corresponding section
        df = df.filter(df[section].isNotNull())

        # add salt
        df = salt_from_last_digits(
            df, colname="{}.{}".format(section, field), npartitions=npartitions
        )

        n_alerts += df.count()

        # Drop unused partitioning columns
        df = df.drop("year").drop("month").drop("day")

        # Drop images
        df = df.drop("cutoutScience").drop("cutoutTemplate").drop("cutoutDifference")

        # push section data to HBase
        ingest_section(
            df,
            major_version=major_version,
            minor_version=minor_version,
            row_key_name=row_key_name,
            table_name=table_name,
            catfolder=catfolder,
        )

    return n_alerts


def ingest_object_data(
    kind,
    paths,
    catfolder,
    major_version,
    minor_version,
    npartitions=1000,
):
    """Remove duplicated and push data to HBase

    Notes
    -----
    The duplicates are based on diaObject.diaObjectId

    Parameters
    ----------
    kind: str
        static or sso alerts.
    paths: list
        List of paths to parquet files on HDFS
    catfolder: str
        Folder to save catalog (saved locally for inspection)
    major_version: int
        LSST alert schema major version (e.g. 9)
    minor_version: int
        LSST alert schema minor version (e.g. 0)
    npartitions: int
        Number of HBase partitions in the table.

    Returns
    -------
    n_alerts: int
        Number of alerts ingested
    table_name: str
        Table name
    """
    assert kind in ["static", "sso"], (
        "kind={} is not recognized. Choose among: static, sso".format(kind)
    )

    df = load_parquet_files(paths)

    if kind == "static":
        section = "diaObject"
        field = "diaObjectId"
        # Name of the rowkey in the table. Should be a column name
        # or a combination of column separated by _ (e.g. jd_objectId).
        row_key_name = "salt_diaObjectId"
        table_name = "rubin.diaObject"
    elif kind == "sso":
        # FIXME: add ssObject when it will be available
        section = "MPCORB"
        field = "ssObjectId"
        # Name of the rowkey in the table. Should be a column name
        # or a combination of column separated by _ (e.g. jd_objectId).
        row_key_name = "salt_ssObjectId"
        table_name = "rubin.ssObject"

    # Keep only rows with corresponding section
    df = df.filter(df[section].isNotNull())

    # add salt
    df = salt_from_last_digits(
        df, colname="{}.{}".format(section, field), npartitions=npartitions
    )

    # Drop unused partitioning columns
    df = df.drop("year").drop("month").drop("day")

    # Drop images
    df = df.drop("cutoutScience").drop("cutoutTemplate").drop("cutoutDifference")

    # Keep only the last alert per object
    w = Window.partitionBy("{}.{}".format(section, field))
    df_dedup = (
        df.withColumn("maxMjd", F.max("diaSource.midpointMjdTai").over(w))
        .where(F.col("diaSource.midpointMjdTai") == F.col("maxMjd"))
        .drop("maxMjd")
    )

    n_alerts = df_dedup.count()

    # push section data to HBase
    ingest_section(
        df_dedup,
        major_version=major_version,
        minor_version=minor_version,
        row_key_name=row_key_name,
        table_name=table_name,
        catfolder=catfolder,
    )

    return n_alerts, table_name


def ingest_section(
    df, major_version, minor_version, row_key_name, table_name, catfolder
):
    """Push diaSource + Fink added values stored in a Spark DataFrame into HBase

    Parameters
    ----------
    df: Spark DataFrame
        Spark DataFrame (full alert schema)
    row_key_name: str
        Name of the rowkey in the table. Should be a column name
        or a combination of column separated by _ (e.g. jd_objectId).
    table_name: str
        HBase table name. Must exist in the cluster.
    catfolder: str
        Folder to save catalog (saved locally for inspection)
    """
    section_name = table_name.split(".")[1]

    (
        root_level,
        diaobject,
        mpcorb,
        diasource,
        sssource,
        fink_source_cols,
        fink_object_cols,
    ) = load_all_rubin_cols(major_version, minor_version)

    # which data to ingest
    if section_name == "diaSource_static":
        sections = [root_level, diasource, fink_source_cols]
    elif section_name == "diaSource_sso":
        sections = [root_level, diasource, sssource]
    elif section_name == "diaObject":
        sections = [root_level, diaobject, fink_object_cols]
    elif section_name == "ssObject":
        # FIXME: add ssObject
        sections = [root_level, mpcorb]
    else:
        _LOG.error(
            "section must be one of 'diaSource_static', 'diaSource_sso', 'diaObject', 'ssObject'. {} is not allowed.".format(
                section_name
            )
        )
        raise ValueError()

    # Check all columns exist, fill if necessary, and cast data
    df_flat, cols, cf = flatten_dataframe(df, sections)

    df_flat = add_row_key(
        df_flat, row_key_name=row_key_name, cols=row_key_name.split("_")
    )

    # FIXME: not sure what it brings after flatten_dataframe
    # df_flat = select_relevant_columns(
    #     df_flat,
    #     row_key_name=row_key_name,
    #     cols=cols,
    # )

    push_to_hbase(
        df=df_flat,
        table_name=table_name,
        rowkeyname=row_key_name,
        cf=cf,
        catfolder=catfolder,
    )


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    globs = globals()
    root = os.environ["FINK_HOME"]

    # globs["rubin_7p4"] = os.path.join(root, "datasim/rubin_test_data_7_4.parquet")

    # Run the Spark test suite
    spark_unit_tests(globs, withstreaming=False)
