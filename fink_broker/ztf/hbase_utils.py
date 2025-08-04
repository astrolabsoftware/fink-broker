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
import os
import logging


import pyspark.sql.functions as F


from fink_science.ztf.xmatch.utils import MANGROVE_COLS
from fink_science.ztf.blazar_low_state.utils import BLAZAR_COLS

from fink_broker.common.hbase_utils import select_relevant_columns
from fink_broker.common.hbase_utils import add_row_key
from fink_broker.common.hbase_utils import push_to_hbase
from fink_broker.common.hbase_utils import flatten_dataframe

from fink_broker.common.tester import spark_unit_tests

_LOG = logging.getLogger(__name__)


def load_fink_cols():
    """Fink-derived columns used in HBase tables with type.

    Returns
    -------
    out: dictionary
        Keys are column names (flattened). Values are data type.

    Examples
    --------
    >>> fink_cols, fink_nested_cols = load_fink_cols()
    >>> print(len(fink_cols))
    32

    >>> print(len(fink_nested_cols))
    7
    """
    fink_cols = {
        "DR3Name": {"type": "string", "default": "Unknown"},
        "Plx": {"type": "float", "default": 0.0},
        "anomaly_score": {"type": "double", "default": 0.0},
        "cdsxmatch": {"type": "string", "default": "Unknown"},
        "e_Plx": {"type": "float", "default": 0.0},
        "gcvs": {"type": "string", "default": "Unknown"},
        "mulens": {"type": "double", "default": 0.0},
        "nalerthist": {"type": "int", "default": 0},
        "rf_kn_vs_nonkn": {"type": "double", "default": 0.0},
        "rf_snia_vs_nonia": {"type": "double", "default": 0.0},
        "roid": {"type": "int", "default": 0},
        "snn_sn_vs_all": {"type": "double", "default": 0.0},
        "snn_snia_vs_nonia": {"type": "double", "default": 0.0},
        "tracklet": {"type": "string", "default": ""},
        "vsx": {"type": "string", "default": "Unknown"},
        "x3hsp": {"type": "string", "default": "Unknown"},
        "x4lac": {"type": "string", "default": "Unknown"},
        "lc_features_g": {"type": "string", "default": "[]"},
        "lc_features_r": {"type": "string", "default": "[]"},
        "jd_first_real_det": {"type": "double", "default": 0.0},
        "jdstarthist_dt": {"type": "double", "default": 0.0},
        "mag_rate": {"type": "double", "default": 0.0},
        "sigma_rate": {"type": "double", "default": 0.0},
        "lower_rate": {"type": "double", "default": 0.0},
        "upper_rate": {"type": "double", "default": 0.0},
        "delta_time": {"type": "double", "default": 0.0},
        "from_upper": {"type": "boolean", "default": False},
        "spicy_id": {"type": "int", "default": -1},
        "spicy_class": {"type": "string", "default": "Unknown"},
        "tns": {"type": "string", "default": ""},
        "gaiaVarFlag": {"type": "int", "default": 0},
        "gaiaClass": {"type": "string", "default": "Unknown"},
    }

    fink_nested_cols = {}
    for col_ in MANGROVE_COLS:
        name = "mangrove.{}".format(col_)
        fink_nested_cols.update({name: {"type": "string", "default": "None"}})

    for col_ in BLAZAR_COLS:
        name = "blazar_stats.{}".format(col_)
        fink_nested_cols.update({name: {"type": "float", "default": 0.0}})

    return fink_cols, fink_nested_cols


def load_all_ztf_cols():
    """Fink/ZTF columns used in HBase tables with type.

    Returns
    -------
    out: dictionary
        Keys are column names (flattened). Values are data type.

    Examples
    --------
    >>> root_level, candidates, fink_cols, fink_nested_cols = load_all_ztf_cols()
    >>> out = {**root_level, **candidates, **fink_cols, **fink_nested_cols}
    >>> print(len(out))
    147
    """
    fink_cols, fink_nested_cols = load_fink_cols()

    root_level = {
        "fink_broker_version": "string",
        "fink_science_version": "string",
        "objectId": "string",
        "publisher": "string",
        "candid": "long",
        "schemavsn": "string",
    }

    candidates = {
        "aimage": "float",
        "aimagerat": "float",
        "bimage": "float",
        "bimagerat": "float",
        "chinr": "float",
        "chipsf": "float",
        "classtar": "float",
        "clrcoeff": "float",
        "clrcounc": "float",
        "clrmed": "float",
        "clrrms": "float",
        "dec": "double",
        "decnr": "double",
        "diffmaglim": "float",
        "distnr": "float",
        "distpsnr1": "float",
        "distpsnr2": "float",
        "distpsnr3": "float",
        "drb": "float",
        "drbversion": "string",
        "dsdiff": "float",
        "dsnrms": "float",
        "elong": "float",
        "exptime": "float",
        "fid": "int",
        "field": "int",
        "fwhm": "float",
        "isdiffpos": "string",
        "jd": "double",
        "jdendhist": "double",
        "jdendref": "double",
        "jdstarthist": "double",
        "jdstartref": "double",
        "magap": "float",
        "magapbig": "float",
        "magdiff": "float",
        "magfromlim": "float",
        "maggaia": "float",
        "maggaiabright": "float",
        "magnr": "float",
        "magpsf": "float",
        "magzpsci": "float",
        "magzpscirms": "float",
        "magzpsciunc": "float",
        "mindtoedge": "float",
        "nbad": "int",
        "ncovhist": "int",
        "ndethist": "int",
        "neargaia": "float",
        "neargaiabright": "float",
        "nframesref": "int",
        "nid": "int",
        "nmatches": "int",
        "nmtchps": "int",
        "nneg": "int",
        "objectidps1": "long",
        "objectidps2": "long",
        "objectidps3": "long",
        "pdiffimfilename": "string",
        "pid": "long",
        "programid": "int",
        "programpi": "string",
        "ra": "double",
        "ranr": "double",
        "rb": "float",
        "rbversion": "string",
        "rcid": "int",
        "rfid": "long",
        "scorr": "double",
        "seeratio": "float",
        "sgmag1": "float",
        "sgmag2": "float",
        "sgmag3": "float",
        "sgscore1": "float",
        "sgscore2": "float",
        "sgscore3": "float",
        "sharpnr": "float",
        "sigmagap": "float",
        "sigmagapbig": "float",
        "sigmagnr": "float",
        "sigmapsf": "float",
        "simag1": "float",
        "simag2": "float",
        "simag3": "float",
        "sky": "float",
        "srmag1": "float",
        "srmag2": "float",
        "srmag3": "float",
        "ssdistnr": "float",
        "ssmagnr": "float",
        "ssnamenr": "string",
        "ssnrms": "float",
        "sumrat": "float",
        "szmag1": "float",
        "szmag2": "float",
        "szmag3": "float",
        "tblid": "long",
        "tooflag": "int",
        "xpos": "float",
        "ypos": "float",
        "zpclrcov": "float",
        "zpmed": "float",
    }

    candidates = {"candidate." + k: v for k, v in candidates.items()}

    return root_level, candidates, fink_cols, fink_nested_cols


def load_ztf_index_cols():
    """Load columns used for index tables (flattened and casted before).

    Returns
    -------
    out: list of string
        List of (flattened) column names

    Examples
    --------
    >>> out = load_ztf_index_cols()
    >>> print(len(out))
    75
    """
    # From `root` or `candidates.`
    common = [
        "objectId",
        "candid",
        "publisher",
        "rcid",
        "chipsf",
        "distnr",
        "ra",
        "dec",
        "jd",
        "fid",
        "nid",
        "field",
        "xpos",
        "ypos",
        "rb",
        "ssdistnr",
        "ssmagnr",
        "ssnamenr",
        "jdstarthist",
        "jdendhist",
        "tooflag",
        "sgscore1",
        "distpsnr1",
        "neargaia",
        "maggaia",
        "nmtchps",
        "diffmaglim",
        "magpsf",
        "sigmapsf",
        "magnr",
        "sigmagnr",
        "magzpsci",
        "isdiffpos",
        "classtar",
        "drb",
        "ndethist",
    ]

    # Add Fink added values
    fink_cols, fink_nested_cols = load_fink_cols()

    fink_cols_names = list(fink_cols.keys())
    common += fink_cols_names

    fink_nested_cols_names = [i.replace(".", "_") for i in fink_nested_cols.keys()]
    common += fink_nested_cols_names

    return common


def load_ztf_crossmatch_cols():
    """Load columns used for the crossmatch table (casted).

    Returns
    -------
    out: list of string
        List of column names casted

    Examples
    --------
    >>> out = load_ztf_crossmatch_cols()
    >>> print(len(out))
    13
    """
    to_use = [
        "objectId",
        "candid",
        "magpsf",
        "sigmapsf",
        "jd",
        "jdstarthist",
        "cdsxmatch",
        "drb",
        "ra",
        "dec",
        "fid",
        "distnr",
        "nalerthist",
    ]

    return to_use


def cast_features(df):
    """Cast feature columns into string of array

    Parameters
    ----------
    df: Spark DataFrame
        DataFrame of alerts

    Returns
    -------
    df: Spark DataFrame

    Examples
    --------
    # Read alert from the raw database
    >>> df = spark.read.format("parquet").load(ztf_alert_sample_scidatabase)

    >>> df = cast_features(df)
    >>> assert 'lc_features_g' in df.columns, df.columns

    >>> a_row = df.select('lc_features_g').limit(1).toPandas().to_numpy()[0][0]
    >>> assert isinstance(a_row, str), a_row
    """
    if ("lc_features_g" in df.columns) and ("lc_features_r" in df.columns):
        df = df.withColumn("lc_features_g", F.array("lc_features_g.*").astype("string"))

        df = df.withColumn("lc_features_r", F.array("lc_features_r.*").astype("string"))

    return df


def push_full_df_to_hbase(df, row_key_name, table_name, catalog_name):
    """Push data stored in a Spark DataFrame into HBase

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
    # Cast feature columns
    df_casted = cast_features(df)

    # Check all columns exist, fill if necessary, and cast data
    df_flat, cols_i, cols_d, cf = flatten_dataframe(df_casted, source="ztf")

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
        catfolder=catalog_name,
    )


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
