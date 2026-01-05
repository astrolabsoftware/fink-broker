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

# from fink_science.ztf.xmatch.utils import MANGROVE_COLS  # FIXME: common
from fink_broker.rubin.science import CAT_PROPERTIES

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
    22

    >>> print(len(fink_object_cols))
    6
    """
    fink_source_cols = {}

    # Crossmatch
    for k in CAT_PROPERTIES.keys():
        for type_, name in zip(
            CAT_PROPERTIES[k]["types"], CAT_PROPERTIES[k]["cols_out"]
        ):
            # Name if either the prefix_col_out or the catalogname otherwise
            fink_source_cols[
                "xm.{}{}_{}".format(
                    CAT_PROPERTIES[k].get("prefix", ""),
                    CAT_PROPERTIES[k].get("prefix_col_out", k),
                    name,
                )
            ] = {"type": type_, "default": None}

    # Classifiers
    # FIXME: how to retrieve automatically names?
    names = ["earlySNIa_score", "snnSnVsOthers_score", "cats_class"]
    types = ["float", "float", "int"]
    for type_, name in zip(types, names):
        fink_source_cols["clf.{}".format(name)] = {
            "type": type_,
            "default": None,
        }

    # Others
    fink_source_cols.update({
        "fink_broker_version": {"type": "string", "default": None},
        "fink_science_version": {"type": "string", "default": None},
        "lsst_schema_version": {"type": "string", "default": None},
    })

    # Predictions
    fink_object_cols = {
        "pred.is_sso": {"type": "boolean", "default": None},
        "pred.is_first": {"type": "boolean", "default": None},
        "pred.is_cataloged": {"type": "boolean", "default": None},
        "pred.main_label_crossmatch": {"type": "string", "default": None},
        # FIXME: Do we want to keep integer?
        "pred.main_label_classifier": {"type": "int", "default": None},
        "misc.firstDiaSourceMjdTai": {"type": "double", "default": None},
    }

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
    3

    >>> assert "observation_reason" in root_level, root_level
    """
    root_level = {
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
    ...     **root_level[1], **diaobject[1],
    ...     **mpcorb[1], **diasource[1],
    ...     **sssource[1], **fink_source_cols[1],
    ...     **fink_object_cols[1]}
    >>> expected = 3 + 82 + 12 + 98 + 24 + 22 + 6
    >>> assert len(out) == expected, (len(out), expected)
    """
    fink_source_cols, fink_object_cols = load_fink_cols()

    root_level = load_rubin_root_level(include_salt=include_salt)

    diasource_schema = extract_avsc_schema("diaSource", major_version, minor_version)
    diasource = {"diaSource." + k: v["type"] for k, v in diasource_schema.items()}

    sssource_schema = extract_avsc_schema("ssSource", major_version, minor_version)
    sssource = {"ssSource." + k: v["type"] for k, v in sssource_schema.items()}

    diaobject_schema = extract_avsc_schema("diaObject", major_version, minor_version)
    diaobject = {"diaObject." + k: v["type"] for k, v in diaobject_schema.items()}

    mpcorb_schema = extract_avsc_schema("mpc_orbits", major_version, minor_version)
    mpcorb = {"mpc_orbits." + k: v["type"] for k, v in mpcorb_schema.items()}

    # FIXME: add ssObject {"sso": ssobject}

    return (
        ["r", root_level],
        ["r", diaobject],
        ["r", diasource],
        ["r", mpcorb],
        ["r", sssource],
        ["f", fink_source_cols],
        ["f", fink_object_cols],
    )


def cast_and_rename_field(colname, coltype, nested):
    """ """
    to_keep = ["xm", "clf"]
    if nested:
        section = colname.split(".")[0]

        if section in to_keep:
            name = F.col(colname).alias(colname.replace(".", "_")).cast(coltype)
        else:
            name = F.col(colname).cast(coltype).alias(colname.split(".")[-1])

        # Assume section.real_colname
        return name
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
        Dictionary with keys being column names and
        corresponding column family as values.
    """
    cols = []
    cf = {}
    all_colnames = []

    for section in sections:
        cf_name = section[0]
        schema = section[1]

        cols_ = []
        for colname, coltype_ in schema.items():
            if colname.split(".")[-1] in all_colnames:
                # This can happen e.g. for ssSource.ssObjectId and diaSource.ssObjectId for table diaSource_sso
                _LOG.warn(
                    "Duplicate detected for {} -- ignoring the second apparition".format(
                        colname
                    )
                )
                continue
            all_colnames.append(colname.split(".")[-1])

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
                _LOG.warn(
                    "Adding a new column with value `{}` and type `{}`".format(
                        default, coltype
                    )
                )
                df = df.withColumn(colname, F.lit(default))
                cols_.append(cast_and_rename_field(colname, coltype, nested))

        # Assign cf ID to columns
        cf.update({i: cf_name for i in df.select(cols_).columns})

        # Update total columns
        cols += cols_

    # flatten all names
    df = df.select(cols)

    return df, cols, cf


def ingest_source_data(
    kind,
    paths,
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
    n_alerts: int
        Number of alerts ingested
    table_name: str
        Table name
    """
    if kind == "static":
        section = "diaObject"
        field = "diaObjectId"
    elif kind == "sso":
        # Use mpc_orbits to make sure mpcDesignation is available
        section = "ssSource"
        field = "ssObjectId"

    # Name of the rowkey in the table. Should be a column name
    # or a combination of column separated by _
    row_key_name = "salt_{}_midpointMjdTai".format(field)
    table_name = "rubin.diaSource_{}".format(field)

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

    return n_alerts, table_name


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
        table_name = "rubin.diaObject"
    elif kind == "sso":
        section = "ssSource"
        field = "ssObjectId"
        table_name = "rubin.ssObject"

    # Name of the rowkey in the table. Should be a column name
    # or a combination of column separated by _
    row_key_name = "salt_{}".format(field)

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
        df
        .withColumn("maxMjd", F.max("diaSource.midpointMjdTai").over(w))
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
    """Push values stored in a Spark DataFrame into HBase

    Notes
    -----
    The fields to push depends on the table name.

    Parameters
    ----------
    df: Spark DataFrame
        Spark DataFrame (full alert schema)
    major_version: int
        LSST alert schema major version (e.g. 9)
    minor_version: int
        LSST alert schema minor version (e.g. 0)
    row_key_name: str
        Name of the rowkey in the table. Should be a column name
        or a combination of column separated by _ (e.g. diaObjectId_midpointMjdTai).
    table_name: str
        HBase table name. Must exist in the cluster.
    catfolder: str
        Folder to save catalog (saved locally for inspection)
    """
    section_name = table_name.split(".")[1]

    (
        root_level,
        diaobject,
        diasource,
        mpcorb,
        sssource,
        fink_source_cols,
        fink_object_cols,
    ) = load_all_rubin_cols(major_version, minor_version)

    # which data to ingest
    if section_name == "diaSource_diaObjectId":
        sections = [root_level, diasource, fink_source_cols]
    elif section_name == "diaSource_ssObjectId":
        sections = [
            root_level,
            diasource,
            sssource,
            ["r", {"mpc_orbits.mpcDesignation": "string"}],  # for the row key
        ]
    elif section_name == "diaObject":
        sections = [root_level, diaobject, fink_object_cols]
    elif section_name == "ssObject":
        # FIXME: add ssObject
        sections = [root_level, mpcorb]
    elif section_name == "pixel128":
        # assuming static objects
        # Contains only the last source for all objects
        sections = [
            root_level,
            diasource,
            diaobject,
            fink_object_cols,
            fink_source_cols,
            ["f", {"pixel128": "int"}],  # for the rowkey
        ]
    else:
        _LOG.error(
            "section must be one of 'diaSource_diaObjectId', 'diaSource_ssObjectId', 'diaObject', 'ssObject', 'pixel128'. {} is not allowed.".format(
                section_name
            )
        )
        raise ValueError()

    # Check all columns exist, fill if necessary, and cast data
    df_flat, _, cf = flatten_dataframe(df, sections)

    df_flat = add_row_key(
        df_flat, row_key_name=row_key_name, cols=row_key_name.split("_")
    )

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
