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
from pyspark.sql.utils import AnalysisException

from fink_broker.common.hbase_utils import select_relevant_columns
from fink_broker.common.hbase_utils import assign_column_family_names
from fink_broker.common.hbase_utils import add_row_key
from fink_broker.common.hbase_utils import push_to_hbase
from fink_broker.common.spark_utils import load_parquet_files

from fink_science.ztf.xmatch.utils import MANGROVE_COLS  # FIXME: common

import numpy as np
import pandas as pd
import os
import logging

_LOG = logging.getLogger(__name__)


def load_fink_cols():
    """Fink-derived columns used in HBase tables with type.

    Returns
    -------
    out: dictionary
        Keys are column names (flattened). Values are data type and default.

    Examples
    --------
    >>> fink_cols, fink_nested_cols = load_fink_cols()
    >>> print(len(fink_cols))
    18
    """
    fink_cols = {
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
        # from ZTF
        # "snn_sn_vs_all": {"type": "double", "default": 0.0},
        # "anomaly_score": {"type": "double", "default": 0.0},
        # "mulens": {"type": "double", "default": 0.0},
        # "nalerthist": {"type": "int", "default": 0},
        # "rf_kn_vs_nonkn": {"type": "double", "default": 0.0},
        # "tracklet": {"type": "string", "default": ""},
        # "lc_features_g": {"type": "string", "default": "[]"},
        # "lc_features_r": {"type": "string", "default": "[]"},
        # "jd_first_real_det": {"type": "double", "default": 0.0},
        # "jdstarthist_dt": {"type": "double", "default": 0.0},
        # "mag_rate": {"type": "double", "default": 0.0},
        # "sigma_rate": {"type": "double", "default": 0.0},
        # "lower_rate": {"type": "double", "default": 0.0},
        # "upper_rate": {"type": "double", "default": 0.0},
        # "delta_time": {"type": "double", "default": 0.0},
        # "from_upper": {"type": "boolean", "default": False},
    }

    fink_nested_cols = {}
    for col_ in MANGROVE_COLS:
        name = "mangrove.{}".format(col_)
        fink_nested_cols.update({name: {"type": "string", "default": "None"}})

    return fink_cols, fink_nested_cols


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
    """
    if isinstance(atype, list):
        # of type ["null", "something else"]
        atype.remove("null")
        return atype[0]
    elif isinstance(atype, str) and atype != "null":
        return atype
    else:
        raise ValueError("Type {} for field {} is not supported".format(atype, name))


def extract_diasource_schema(major_version, minor_version):
    """Convert avsc into useful dictionary

    Parameters
    ----------
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
    >>> schema = extract_diasource_schema(7, 4)
    >>> len(schema)
    140

    >>> schema["psfFlux"]
    {'type': 'float', 'default': None}
    """
    _LOG.info("diaSource schema version {}.{}".format(major_version, minor_version))

    baseurl = "https://raw.githubusercontent.com/lsst/alert_packet/refs/heads/main/python/lsst/alert/packet/schema"
    pdf = pd.read_json(
        os.path.join(
            baseurl,
            "{}/{}".format(major_version, minor_version),
            "lsst.v{}_{}.diaSource.avsc".format(major_version, minor_version),
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


def load_all_cols(major_version, minor_version):
    """Fink/ZTF columns used in HBase tables with type.

    Returns
    -------
    out: dictionary
        Keys are column names (flattened). Values are data type.

    Examples
    --------
    >>> root_level, diasource, fink_cols, fink_nested_cols = load_all_cols(7, 4)
    >>> out = {**root_level, **diasource, **fink_cols, **fink_nested_cols}
    >>> assert len(out) == 3 + 140 + 18 + 4
    """
    fink_cols, fink_nested_cols = load_fink_cols()

    root_level = {
        "fink_broker_version": "string",
        "fink_science_version": "string",
        # "lsst_schema_version": "string", # FIXME: to be added in stream2raw ? Or in raw2science ?
        "alertId": "long",  # FIXME: there should be diaObjectId
        "salt": "string",  # partitioning key
    }

    diasource_schema = extract_diasource_schema(major_version, minor_version)
    diasource = {"diaSource." + k: v["type"] for k, v in diasource_schema.items()}

    return root_level, diasource, fink_cols, fink_nested_cols


def flatten_dataframe(df, major_version, minor_version):
    """Flatten DataFrame columns for HBase ingestion

    Notes
    -----
    Check also all Fink columns exist, fill if necessary, and cast all columns

    Parameters
    ----------
    df: DataFrame
        Spark DataFrame with raw alert data

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
    root_level, diasource, fink_cols, fink_nested_cols = load_all_cols(
        major_version, minor_version
    )

    tmp_i = []
    tmp_d = []

    # assuming no missing columns
    for colname, coltype in root_level.items():
        tmp_i.append(F.col(colname).cast(coltype))

    # assuming no missing columns
    for colname, coltype in diasource.items():
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


def incremental_ingestion_with_salt(
    paths,
    table_name,
    row_key_name,
    ingestor,
    catfolder,
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
    paths: list
        List of paths to parquet files on HDFS
    table_name: str
        HBase table name, in the form `rubin.<suffix>`.
        Must exist in the cluster.
    row_key_name: str
        Name of the rowkey in the table. Should be a column name
        or a combination of column separated by _ (e.g. jd_objectId).
    ingestor: func
        Function to ingest data. Should correspond to `<suffix>`
    catfolder: str
        Folder to save catalog (saved locally for inspection)
    nfiles: int
        Number of parquet files to ingest at once
    npartitions: int
        Number of HBase partitions in the table.

    Returns
    -------
    out: int
        Number of alerts ingested
    """
    n_alerts = 0
    for index in range(0, len(paths), nfiles):
        df = load_parquet_files(paths[index : index + nfiles])

        # Key prefix will be the last N digits
        # This must match the number of partitions in the table
        ndigits = int(np.log10(npartitions)) + 1
        df = df.withColumn(
            "salt",
            F.lpad(
                F.substring("diaObject.diaObjectId", -ndigits, ndigits), ndigits, "0"
            ),
        )

        n_alerts += df.count()

        # Drop unused partitioning columns
        df = df.drop("year").drop("month").drop("day")

        # Drop images
        df = df.drop("cutoutScience").drop("cutoutTemplate").drop("cutoutDifference")

        # push diaSource data to HBase
        ingestor(
            df,
            major_version=7,  # FIXME: should be programmatic from alert packet
            minor_version=4,  # FIXME: should be programmatic from alert packet
            row_key_name=row_key_name,
            table_name=table_name,
            catfolder=catfolder,
        )

    return n_alerts


def ingest_diasource(
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
    # Check all columns exist, fill if necessary, and cast data
    df_flat, cols_i, cols_d, cf = flatten_dataframe(df, major_version, minor_version)

    df_flat = add_row_key(
        df_flat, row_key_name=row_key_name, cols=row_key_name.split("_")
    )

    # Flatten columns
    df_flat = select_relevant_columns(
        df_flat,
        row_key_name=row_key_name,
        cols=cols_i + cols_d,
    )

    push_to_hbase(
        df=df_flat,
        table_name=table_name,
        rowkeyname=row_key_name,
        cf=cf,
        catfolder=catfolder,
    )
