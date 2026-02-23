# Copyright 2019-2026 AstroLab Software
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
from fink_broker.common.hbase_utils import salt_from_mpc_designation
from fink_broker.common.spark_utils import load_parquet_files
from fink_broker.common.spark_utils import connect_to_raw_database
from fink_broker.common.spark_utils import ang2pix

from fink_broker.rubin.science import CAT_PROPERTIES
from fink_broker.common.tester import spark_unit_tests

# from fink_science.rubin.ad_features.processor import LSST_FILTER_LIST, FEATURES_COLS
import fink_filters.rubin.livestream as ffrl

import pandas as pd
import os
import logging
import pkgutil

_LOG = logging.getLogger(__name__)

userfilters = [
    "{}.{}.filter.{}".format(ffrl.__package__, mod, mod.split("filter_")[1])
    for _, mod, _ in pkgutil.iter_modules(ffrl.__path__)
]

# In-line with the definition of tags in bin/rubin/distribute.py
ALLOWED_TAGS = ["tag_" + userfilter.split(".")[-1] for userfilter in userfilters]


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
    30

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
    names = [
        "earlySNIa_score",
        "snnSnVsOthers_score",
        "cats_class",
        "cats_score",
        "elephant_kstest_science",
        "elephant_kstest_template",
    ]
    types = ["float", "float", "int", "float", "float", "float"]
    for type_, name in zip(types, names):
        fink_source_cols["clf.{}".format(name)] = {
            "type": type_,
            "default": None,
        }

    # FIXME: do not push lc features for the moment
    # # LC features
    # for band in LSST_FILTER_LIST:
    #     for name in FEATURES_COLS:
    #         fink_source_cols["{}_lc_feat.{}".format(band, name)] = {
    #             "type": "float",
    #             "default": None,
    #         }

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
        "misc.firstDiaSourceMjdTaiFink": {"type": "double", "default": None},
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
    >>> schema = extract_avsc_schema("diaSource", 10, 0)
    >>> len(schema)
    98

    >>> schema["psfFlux"]
    {'type': 'float', 'default': None}

    >>> schema = extract_avsc_schema("diaObject", 10, 0)
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
    >>> root_level, diaobject, mpcorb, diasource, sssource, fink_source_cols, fink_object_cols = load_all_rubin_cols(10, 0)
    >>> out = {
    ...     **root_level[1], **diaobject[1],
    ...     **mpcorb[1], **diasource[1],
    ...     **sssource[1], **fink_source_cols[1],
    ...     **fink_object_cols[1]}
    >>> expected = 3 + 82 + 98 + 14 + 53 + 39 + 30 + 6
    >>> assert len(out) == expected, (len(out), expected)
    """
    fink_source_cols, fink_object_cols = load_fink_cols()

    root_level = load_rubin_root_level(include_salt=include_salt)

    diasource_schema = extract_avsc_schema("diaSource", major_version, minor_version)
    diasource = {"diaSource." + k: v["type"] for k, v in diasource_schema.items()}

    fp_schema = extract_avsc_schema("diaForcedSource", major_version, minor_version)
    fp = {"diaForcedSource." + k: v["type"] for k, v in fp_schema.items()}

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
        ["r", fp],
        ["r", mpcorb],
        ["r", sssource],
        ["f", fink_source_cols],
        ["f", fink_object_cols],
    )


def cast_and_rename_field(colname, coltype, nested):
    """ """
    to_keep = [
        "xm",
        "clf",
        # *["{}_lc_feat".format(band) for band in LSST_FILTER_LIST],
    ]
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
                default = coltype_.get("default", None)
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


def format_for_hbase(df, section, field, kind, npartitions):
    """Initial formatting for HBase ingestion

    Notes
    -----
    This adds salt to the row key, and drop images. It
    also keep alerts for which df[section].isNotNull()

    Parameters
    ----------
    df: Spark DataFrame
        Alert DataFrame
    section: str
       Section of the schema to use that
       hosts `field`. Use to generate the salt.
    field: str
        Field contains in `section`. Use to generate the salt.
    kind: str
        static or sso alerts. Defined the type of salt.
    npartitions: int
        Number of partitions in the targeted HBase table

    Returns
    -------
    out: Spark DataFrame
        The initial dataframe with the salt column,
        and without images.
    """
    # Keep only rows with corresponding section
    df = df.filter(df[section].isNotNull())

    # add salt
    if kind == "static":
        df = salt_from_last_digits(
            df, colname="{}.{}".format(section, field), npartitions=npartitions
        )
    elif kind == "sso":
        df = salt_from_mpc_designation(df, colname="{}.{}".format(section, field))

    # Drop unused partitioning columns
    df = df.drop("year").drop("month").drop("day")

    # Drop images
    df = df.drop("cutoutScience").drop("cutoutTemplate").drop("cutoutDifference")

    return df


def ingest_source_data(
    kind,
    paths,
    catfolder,
    major_version,
    minor_version,
    nfiles=100,
    npartitions=1000,
    streaming=False,
    checkpoint_path="",
):
    """Push data to HBase by batch of parquet files

    Notes
    -----
    For static data the row key is salted using the last 3 digits
    of diaObject.diaObjectId. For SSO, the row key is salted using
    the 2 digits corresponding to the year from the packed designation.

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
    streaming: bool
        If True, ingest data in real-time assuming df is a
        streaming DataFrame. Default is False (static DataFrame).
    checkpoint_path: str
        Path to the checkpoint for streaming. Only relevant if `streaming=True`

    Returns
    -------
    n_alerts: int
        Number of alerts ingested. None if streaming=True.
    table_name: str
        Table name
    hbase_query: StreamingQuery
        The Spark streaming query. None if streaming=False.
    """
    if kind == "static":
        section = "diaObject"
        field = "diaObjectId"
    elif kind == "sso":
        section = "mpc_orbits"
        field = "packed_primary_provisional_designation"

    # Name of the rowkey in the table. Should be a column name
    # or a combination of column separated by _
    cols_row_key_name = ["salt", field, "midpointMjdTai"]
    row_key_name = "_".join(cols_row_key_name)
    table_name = "rubin.diaSource_{}".format(kind)

    if streaming:
        # streaming ingestion
        df = connect_to_raw_database(paths, paths, latestfirst=False)
        df = format_for_hbase(df, section, field, kind, npartitions)

        # push section data to HBase
        hbase_query = ingest_section(
            df,
            major_version=major_version,
            minor_version=minor_version,
            row_key_name=row_key_name,
            table_name=table_name,
            catfolder=catfolder,
            cols_row_key_name=cols_row_key_name,
            streaming=streaming,
            checkpoint_path=checkpoint_path,
        )
        n_alerts = None
    else:
        # static ingestion
        nloops = int(len(paths) / nfiles) + 1
        n_alerts = 0

        for index in range(0, len(paths), nfiles):
            _LOG.info("Loop {}/{}".format(index + 1, nloops))

            df = load_parquet_files(paths[index : index + nfiles])

            df = format_for_hbase(df, section, field, kind, npartitions)

            n_alerts += df.count()

            # push section data to HBase
            hbase_query = ingest_section(
                df,
                major_version=major_version,
                minor_version=minor_version,
                row_key_name=row_key_name,
                table_name=table_name,
                catfolder=catfolder,
                cols_row_key_name=cols_row_key_name,
                streaming=streaming,
                checkpoint_path=checkpoint_path,
            )

    return n_alerts, table_name, hbase_query


def ingest_object_data(
    kind,
    paths,
    catfolder,
    major_version,
    minor_version,
    npartitions=1000,
    streaming=False,
    checkpoint_path="",
):
    """Remove duplicated and push data to HBase

    Notes
    -----
    The duplicates are based on diaObject.diaObjectId.
    They are removed only for streaming=False.

    Notes
    -----
    How to make the streaming working while there is
    a need to drop duplicates? Probably for streaming
    there should be no duplicates removal.

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
    streaming: bool
        If True, ingest data in real-time assuming df is a
        streaming DataFrame. Default is False (static DataFrame).
    checkpoint_path: str
        Path to the checkpoint for streaming. Only relevant if `streaming=True`

    Returns
    -------
    n_alerts: int
        Number of alerts ingested. None if streaming=True.
    table_name: str
        Table name
    hbase_query: StreamingQuery
        The Spark streaming query. None if streaming=False.
    """
    assert kind in ["static", "sso"], (
        "kind={} is not recognized. Choose among: static, sso".format(kind)
    )

    if streaming:
        df = connect_to_raw_database(paths, paths, latestfirst=False)
    else:
        df = load_parquet_files(paths)

    if kind == "static":
        section = "diaObject"
        field = "diaObjectId"
        table_name = "rubin.diaObject"
    elif kind == "sso":
        section = "mpc_orbits"
        field = "packed_primary_provisional_designation"
        table_name = "rubin.mpc_orbits"

    # Name of the rowkey in the table. Should be a column name
    # or a combination of column separated by _
    cols_row_key_name = ["salt", field]
    row_key_name = "_".join(cols_row_key_name)

    df = format_for_hbase(df, section, field, kind, npartitions)

    if not streaming:
        # Keep only the last alert per object
        w = Window.partitionBy("{}.{}".format(section, field)).rowsBetween(
            Window.unboundedPreceding, Window.unboundedFollowing
        )
        df_dedup = (
            df
            .withColumn("maxMjd", F.max("diaSource.midpointMjdTai").over(w))
            .where(F.col("diaSource.midpointMjdTai") == F.col("maxMjd"))
            .drop("maxMjd")
        )

        n_alerts = df_dedup.count()
    else:
        df_dedup = df
        n_alerts = None

    # push section data to HBase
    hbase_query = ingest_section(
        df_dedup,
        major_version=major_version,
        minor_version=minor_version,
        row_key_name=row_key_name,
        table_name=table_name,
        catfolder=catfolder,
        cols_row_key_name=cols_row_key_name,
        streaming=streaming,
        checkpoint_path=checkpoint_path,
    )

    return n_alerts, table_name, hbase_query


def ingest_section(
    df,
    major_version,
    minor_version,
    row_key_name,
    table_name,
    catfolder,
    cols_row_key_name=None,
    streaming=False,
    checkpoint_path="",
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
    cols_row_key_name: list, optional
        List of columns to use for the row key. If None (default),
        split the row key using _. Only used for SSO for which
        one column name contains _.
    streaming: bool
        If True, ingest data in real-time assuming df is a
        streaming DataFrame. Default is False (static DataFrame).
    checkpoint_path: str
        Path to the checkpoint for streaming. Only relevant if `streaming=True`

    Returns
    -------
    query: StreamingQuery or None
        StreamingQuery if `streaming=True`, None otherwise.
    """
    if cols_row_key_name is None:
        cols_row_key_name = row_key_name.split("_")

    section_name = table_name.split(".")[1]

    (
        root_level,
        diaobject,
        diasource,
        fp,
        mpcorb,
        sssource,
        fink_source_cols,
        fink_object_cols,
    ) = load_all_rubin_cols(major_version, minor_version)

    # which data to ingest
    if section_name == "diaSource_static":
        sections = [root_level, diasource, fink_source_cols]
    elif section_name == "diaSource_sso":
        sections = [
            root_level,
            diasource,
            sssource,
            [
                "r",
                {"mpc_orbits.packed_primary_provisional_designation": "string"},
            ],  # for the row key
        ]
    elif section_name == "diaObject":
        sections = [root_level, diaobject, fink_object_cols]
    elif section_name == "fp":
        sections = [fp]
    elif section_name == "mpc_orbits":
        # FIXME: add ssObject
        sections = [root_level, mpcorb]
    elif section_name == "pixel1024":
        # assuming static objects
        # Contains only the last source for all objects
        sections = [
            root_level,
            diasource,
            diaobject,
            fink_object_cols,
            fink_source_cols,
            ["f", {"pixel1024": "int"}],  # for the rowkey
        ]
    elif section_name in ALLOWED_TAGS:
        # FIXME: Does not work for SSO filters!
        sections = [root_level, diasource, fink_source_cols]
    else:
        _LOG.error(
            "section must be one of 'diaSource_static', 'diaSource_sso', 'diaObject', 'mpc_orbits', 'pixel1024', 'fp', or a filter: {}. {} is not allowed.".format(
                ALLOWED_TAGS,
                section_name,
            )
        )
        raise ValueError()

    # Check all columns exist, fill if necessary, and cast data
    df_flat, _, cf = flatten_dataframe(df, sections)

    df_flat = add_row_key(df_flat, row_key_name=row_key_name, cols=cols_row_key_name)

    query = push_to_hbase(
        df=df_flat,
        table_name=table_name,
        rowkeyname=row_key_name,
        cf=cf,
        catfolder=catfolder,
        streaming=streaming,
        checkpoint_path=checkpoint_path,
    )

    return query


def format_cutouts_for_hbase(df, row_key_name, npartitions):
    """Initial formatting for ingestion of cutouts in HBase

    Parameters
    ----------
    df: Spark DataFrame
        Alert DataFrame
    row_key_name: str
        Name of the rowkey in the table. Should be a column name
        or a combination of column separated by _ (e.g. diaObjectId_midpointMjdTai).
    npartitions: int
        Number of partitions in the targeted HBase table

    Returns
    -------
    out: Spark DataFrame
        The initial dataframe with the salt column,
        and without images.
    cf: dict
        Dictionary for column family
    """
    df = df.withColumn("hdfs_path", F.input_file_name())

    # add salt based on diaSourceId
    df = salt_from_last_digits(
        df, colname="diaSource.diaSourceId", npartitions=npartitions
    )

    cols = [
        "diaObject.diaObjectId",
        "diaSource.midpointMjdTai",
        "diaSource.diaSourceId",
        "hdfs_path",
        "salt",
    ]

    df = df.select(cols)

    cf = {
        "diaObjectId": "r",
        "diaSourceId": "r",
        "midpointMjdTai": "r",
        "hdfs_path": "r",
    }

    df = add_row_key(df, row_key_name=row_key_name, cols=row_key_name.split("_"))

    # Not needed anymore
    df = df.drop("salt")

    return df, cf


def ingest_cutout_metadata(
    paths,
    table_name,
    catfolder,
    npartitions,
    streaming=False,
    checkpoint_path="",
):
    """Ingest cutout data into HBase

    Notes
    -----
    Used in streaming, this pushes paths pointing to `online` during the night.
    One still needs to re-run it (static mode) after merging files in `archive`.

    Parameters
    ----------
    paths: list
        List of paths to parquet files on HDFS
    table_name: str
        Name of the HBase table
    catfolder: str
        Folder to save catalog (saved locally for inspection)
    npartitions: int
        Number of HBase partitions in the table.
    streaming: bool
        If True, ingest data in real-time assuming df is a
        streaming DataFrame. Default is False (static DataFrame).
    checkpoint_path: str
        Path to the checkpoint for streaming. Only relevant if `streaming=True`

    Returns
    -------
    hbase_query: StreamingQuery
        The Spark streaming query. None if streaming=False.
    """
    row_key_name = "salt_diaSourceId"

    if streaming:
        df = connect_to_raw_database(paths, paths, latestfirst=False)
        df, cf = format_cutouts_for_hbase(df, row_key_name, npartitions=npartitions)
        query = push_to_hbase(
            df=df,
            table_name=table_name,
            rowkeyname=row_key_name,
            cf=cf,
            catfolder=catfolder,
            streaming=streaming,
            checkpoint_path=checkpoint_path,
        )
    else:
        nfiles = 100
        nloops = int(len(paths) / nfiles) + 1

        _LOG.info(
            "{} parquet detected ({} loops to perform)".format(len(paths), nloops)
        )

        for index in range(0, len(paths), nfiles):
            _LOG.info("Loop {}/{}".format(index + 1, nloops))
            df = load_parquet_files(paths[index : index + nfiles])
            df, cf = format_cutouts_for_hbase(df, row_key_name, npartitions=npartitions)

            query = push_to_hbase(
                df=df,
                table_name=table_name,
                rowkeyname=row_key_name,
                cf=cf,
                catfolder=catfolder,
                streaming=streaming,
                checkpoint_path=checkpoint_path,
            )

    return query


def format_pixels_for_hbase(df, nside, streaming):
    """Initial formatting for ingestion of pixels in HBase

    Notes
    -----
    This filters SSO objects, and add a column `pixel<NSIDE>`.
    In addition, if streaming=False, apply deduplication based
    on diaObjectId.

    Parameters
    ----------
    df: Spark DataFrame
        Alert DataFrame
    nside: int
        Healpix NSIDE as integer
    streaming: bool
        If False, apply deduplication on diaObjectId.

    Returns
    -------
    out: Spark DataFrame
    """
    # Keep only alerts with diaObject -- this will effectively discard SSO
    df = df.filter(df["diaObject"].isNotNull())

    df = df.withColumn(
        "pixel{}".format(nside),
        ang2pix(df["diaSource.ra"], df["diaSource.dec"], F.lit(nside)),
    )

    if not streaming:
        # Keep only the last alert per object
        w = Window.partitionBy("{}.{}".format("diaObject", "diaObjectId")).rowsBetween(
            Window.unboundedPreceding, Window.unboundedFollowing
        )
        df_dedup = (
            df
            .withColumn("maxMjd", F.max("diaSource.midpointMjdTai").over(w))
            .where(F.col("diaSource.midpointMjdTai") == F.col("maxMjd"))
            .drop("maxMjd")
        )
    else:
        df_dedup = df

    return df_dedup


def ingest_pixels(
    paths,
    table_name,
    catfolder,
    major_version,
    minor_version,
    npartitions,
    streaming=False,
    checkpoint_path="",
):
    """Ingest pixels data into HBase

    Parameters
    ----------
    paths: list
        List of paths to parquet files on HDFS
    table_name: str
        Name of the HBase table
    catfolder: str
        Folder to save catalog (saved locally for inspection)
    major_version: int
        LSST alert schema major version (e.g. 9)
    minor_version: int
        LSST alert schema minor version (e.g. 0)
    npartitions: int
        Number of HBase partitions in the table.
    streaming: bool
        If True, ingest data in real-time assuming df is a
        streaming DataFrame. Default is False (static DataFrame).
    checkpoint_path: str
        Path to the checkpoint for streaming. Only relevant if `streaming=True`

    Returns
    -------
    hbase_query: StreamingQuery
        The Spark streaming query. None if streaming=False.
    """
    # Name of the rowkey in the table. Should be a column name
    # or a combination of column separated by _
    cols_row_key_name = ["pixel1024", "firstDiaSourceMjdTaiFink", "diaObjectId"]
    row_key_name = "_".join(cols_row_key_name)
    nside = int(cols_row_key_name[0].split("pixel")[1])

    if streaming:
        df = connect_to_raw_database(paths, paths, latestfirst=False)
        df = format_pixels_for_hbase(df, nside, streaming)

        query = ingest_section(
            df=df,
            major_version=major_version,
            minor_version=minor_version,
            row_key_name=row_key_name,
            table_name=table_name,
            catfolder=catfolder,
            streaming=streaming,
            checkpoint_path=checkpoint_path,
        )
    else:
        nfiles = 100
        nloops = int(len(paths) / nfiles) + 1

        _LOG.info(
            "{} parquet detected ({} loops to perform)".format(len(paths), nloops)
        )

        # incremental push
        for index in range(0, len(paths), nfiles):
            _LOG.info("Loop {}/{}".format(index + 1, nloops))
            df = load_parquet_files(paths[index : index + nfiles])

            df = format_pixels_for_hbase(df, nside, streaming)

            query = ingest_section(
                df=df,
                major_version=major_version,
                minor_version=minor_version,
                row_key_name=row_key_name,
                cols_row_key_name=cols_row_key_name,
                table_name=table_name,
                catfolder=catfolder,
                streaming=streaming,
                checkpoint_path=checkpoint_path,
            )

    return query


def format_fp_for_hbase(df, npartitions):
    """Initial formatting for HBase ingestion

    Notes
    -----
    This select only new fp records to push, and adds salt to the row key.
    It effectively explodes the column `prvDiaForcedSources` and select
    only records not yet pushed.

    Parameters
    ----------
    df: Spark DataFrame
        Alert DataFrame
    npartitions: int
        Number of partitions in the targeted HBase table

    Returns
    -------
    out: Spark DataFrame
        The initial dataframe with fp explosed, and the salt column
    """
    # Step 1 -- keep records with fp
    df_nonnull = (
        df
        .filter(F.col("diaObject.nDiaSources") > 1)
        .filter(F.col("prvDiaForcedSources").isNotNull())
        .filter(F.size("prvDiaForcedSources") > 0)
    )

    # step 2 -- set maxMidpointMjdTai
    df_withlast = df_nonnull.withColumn(
        "maxMidpointMjdTai",
        F.when(
            F.size("prvDiaSources") == 2,
            0.0,  # Return ridiculously small number to not filter out afterwards
        ).otherwise(
            F.expr(
                "aggregate(prvDiaSources, cast(-1.0 as double), (acc, x) -> greatest(acc, x.midpointMjdTai))"
            )
        ),
    )

    # step 3 -- filter
    df_filtered = df_withlast.filter(~F.col("maxMidpointMjdTai").isNull()).withColumn(
        "prvDiaForcedSources2",
        F.expr(
            "filter(prvDiaForcedSources, x -> x.midpointMjdTai >= maxMidpointMjdTai)"
        ),
    )

    df_ingest = (
        df_filtered
        .select("prvDiaForcedSources2")
        .select(F.explode("prvDiaForcedSources2").alias("forcedSource"))
        .select("forcedSource.*")
    )

    # add salt
    df_ingest = salt_from_last_digits(
        df_ingest, colname="diaObjectId", npartitions=npartitions
    )

    return df_ingest


def ingest_fp(
    paths,
    table_name,
    catfolder,
    major_version,
    minor_version,
    npartitions,
    streaming=False,
    checkpoint_path="",
):
    """Ingest forced photometry data into HBase

    Parameters
    ----------
    paths: list
        List of paths to parquet files on HDFS
    table_name: str
        Name of the HBase table
    catfolder: str
        Folder to save catalog (saved locally for inspection)
    major_version: int
        LSST alert schema major version (e.g. 9)
    minor_version: int
        LSST alert schema minor version (e.g. 0)
    npartitions: int
        Number of HBase partitions in the table.
    streaming: bool
        If True, ingest data in real-time assuming df is a
        streaming DataFrame. Default is False (static DataFrame).
    checkpoint_path: str
        Path to the checkpoint for streaming. Only relevant if `streaming=True`

    Returns
    -------
    hbase_query: StreamingQuery
        The Spark streaming query. None if streaming=False.
    """
    # Name of the rowkey in the table. Should be a column name
    # or a combination of column separated by _
    cols_row_key_name = ["salt", "diaObjectId", "midpointMjdTai"]
    row_key_name = "_".join(cols_row_key_name)

    if streaming:
        df = connect_to_raw_database(paths, paths, latestfirst=False)
        df = format_fp_for_hbase(df, npartitions)

        query = ingest_section(
            df=df,
            major_version=major_version,
            minor_version=minor_version,
            row_key_name=row_key_name,
            table_name=table_name,
            catfolder=catfolder,
            streaming=streaming,
            checkpoint_path=checkpoint_path,
        )
    else:
        nfiles = 100
        nloops = int(len(paths) / nfiles) + 1

        _LOG.info(
            "{} parquet detected ({} loops to perform)".format(len(paths), nloops)
        )

        # incremental push
        for index in range(0, len(paths), nfiles):
            _LOG.info("Loop {}/{}".format(index + 1, nloops))
            df = load_parquet_files(paths[index : index + nfiles])

            df = format_fp_for_hbase(df, npartitions)

            query = ingest_section(
                df=df,
                major_version=major_version,
                minor_version=minor_version,
                row_key_name=row_key_name,
                cols_row_key_name=cols_row_key_name,
                table_name=table_name,
                catfolder=catfolder,
                streaming=streaming,
                checkpoint_path=checkpoint_path,
            )

    return query


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    globs = globals()
    root = os.environ["FINK_HOME"]

    # globs["rubin_7p4"] = os.path.join(root, "datasim/rubin_test_data_7_4.parquet")

    # Run the Spark test suite
    spark_unit_tests(globs, withstreaming=False)
