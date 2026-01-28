# Copyright 2020-2025 AstroLab Software
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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import os
import logging
from itertools import chain

from fink_utils.spark.utils import concat_col

from fink_broker.common.tester import spark_unit_tests

# Import of science modules
from fink_science.rubin.ad_features.processor import extract_features_ad
from fink_science.rubin.cats.processor import predict_nn
from fink_science.rubin.snn.processor import snn_ia_elasticc
from fink_science.rubin.xmatch.processor import xmatch_cds
from fink_science.rubin.xmatch.processor import xmatch_tns
from fink_science.rubin.xmatch.processor import crossmatch_other_catalog
from fink_science.rubin.xmatch.processor import crossmatch_mangrove
from fink_science.rubin.xmatch.utils import MANGROVE_COLS
from fink_science.rubin.random_forest_snia.processor import rfscore_rainbow_elasticc_nometa
from fink_science.rubin.hostless_detection.processor import run_potential_hostless
# from fink_science.rubin.slsn.processor import slsn_rubin

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_LOG = logging.getLogger(__name__)

CAT_PROPERTIES = {
    "simbad": {
        "kind": "cds",
        "cols_out": ["otype"],
        "types": ["string"],
        "distmaxarcsec": 1.0,
    },
    "vizier:I/355/gaiadr3": {
        "kind": "cds",
        "prefix_col_out": "gaiadr3",
        "cols_out": ["DR3Name", "Plx", "e_Plx", "VarFlag"],
        "types": ["string", "float", "float", "string"],
        "distmaxarcsec": 1.0,
    },
    "vizier:VII/292/south": {
        "kind": "cds",
        "prefix_col_out": "legacydr8",
        "cols_out": ["zphot", "e_zphot", "pstar", "fqual"],
        "types": ["float", "float", "float", "int"],
        "distmaxarcsec": 1.5,
    },
    "vizier:B/vsx/vsx": {
        "kind": "cds",
        "prefix_col_out": "vsx",
        "cols_out": ["Type"],
        "types": ["string"],
        "distmaxarcsec": 1.5,
    },
    "vizier:J/ApJS/254/33/table1": {
        "kind": "cds",
        "prefix_col_out": "spicy",
        "cols_out": ["SPICY", "class"],
        "types": ["int", "string"],
        "distmaxarcsec": 1.2,
    },
    "tns": {
        "kind": "tns",
        "cols_out": ["type"],
        "types": ["string"],
        "distmaxarcsec": 1.5,
    },
    "gcvs": {
        "kind": "internal",
        "cols_out": ["type"],
        "types": ["string"],
        "prefix": "",
        "distmaxarcsec": 1.5,
    },
    "3hsp": {
        "kind": "internal",
        "cols_out": ["type"],
        "types": ["string"],
        "prefix": "x",
        "distmaxarcsec": 60.0,
    },
    "4lac": {
        "kind": "internal",
        "cols_out": ["type"],
        "types": ["string"],
        "prefix": "x",
        "distmaxarcsec": 60.0,
    },
    "mangrove": {
        "kind": "mangrove",
        "cols_out": ["HyperLEDA_name", "2MASS_name", "lum_dist", "ang_dist"],
        "types": ["string", "string", "float", "float"],
        "distmaxarcsec": 60.0,
    },
}


def apply_all_xmatch(df, tns_raw_output):
    """Apply all xmatch to a DataFrame

    Parameters
    ----------
    df: Spark DataFrame
        Spark DataFrame containing alert data
    tns_raw_output: str
        If provided, local path to the TNS catalog.

    Returns
    -------
    out: Spark DataFrame

    Examples
    --------
    >>> from fink_broker.common.spark_utils import load_parquet_files
    >>> df = load_parquet_files(rubin_alert_sample)
    >>> cols_in = df.columns
    >>> df = apply_all_xmatch(df, tns_raw_output="")
    >>> cols_out = df.columns

    >>> new_cols = [col for col in cols_out if col not in cols_in]
    >>> assert len(new_cols) == 16, (new_cols, cols_out)

    # apply_science_modules is lazy, so trigger the computation
    >>> an_alert = df.take(1)
    """
    alert_id = "diaSource.diaSourceId"
    ra = "diaSource.ra"
    dec = "diaSource.dec"

    for catname in CAT_PROPERTIES.keys():
        _LOG.info("New xmatch: {}".format(catname))

        if CAT_PROPERTIES[catname]["kind"] == "cds":
            df = xmatch_cds(
                df,
                catalogname=catname,
                prefix_col_out=CAT_PROPERTIES[catname].get("prefix_col_out", None),
                distmaxarcsec=CAT_PROPERTIES[catname]["distmaxarcsec"],
                cols_out=CAT_PROPERTIES[catname]["cols_out"],
                types=CAT_PROPERTIES[catname]["types"],
            )
        elif CAT_PROPERTIES[catname]["kind"] == "tns":
            df = xmatch_tns(
                df,
                distmaxarcsec=CAT_PROPERTIES[catname]["distmaxarcsec"],
                tns_raw_output=tns_raw_output,
            )
        elif CAT_PROPERTIES[catname]["kind"] == "mangrove":
            df = df.withColumn(
                catname,
                crossmatch_mangrove(
                    df[alert_id],
                    df[ra],
                    df[dec],
                    F.lit(CAT_PROPERTIES[catname]["distmaxarcsec"]),
                ),
            )
            # Explode mangrove
            for col_ in MANGROVE_COLS:
                df = df.withColumn(
                    "mangrove_{}".format(col_),
                    df["mangrove"].getItem(col_),
                )
            df = df.drop("mangrove")
        elif CAT_PROPERTIES[catname]["kind"] == "internal":
            df = df.withColumn(
                "{}{}_{}".format(
                    CAT_PROPERTIES[catname]["prefix"],
                    catname,
                    CAT_PROPERTIES[catname]["cols_out"][0],
                ),
                crossmatch_other_catalog(
                    df[alert_id],
                    df[ra],
                    df[dec],
                    F.lit(catname),
                    F.lit(CAT_PROPERTIES[catname]["distmaxarcsec"]),
                ),
            )

    # VarFlag is a string. Make it integer
    # 0=NOT_AVAILABLE / 1=VARIABLE
    df = df.withColumn(
        "gaiadr3_VarFlag",
        F.when(df["gaiadr3_VarFlag"] == "VARIABLE", 1).otherwise(0),
    )

    return df


def apply_science_modules(df: DataFrame, tns_raw_output: str = "") -> DataFrame:
    """Load and apply Fink science modules to enrich Rubin alert content

    Currently available:
    - xmatch
    - CBPF (broad)
    - SNN (Ia)
    - EarlySN Ia

    Parameters
    ----------
    df: DataFrame
        Spark (Streaming or SQL) DataFrame containing raw alert data
    tns_raw_output: str, optional
        Folder that contains raw TNS catalog. Inside, it is expected
        to find the file `tns_raw.parquet` downloaded using
        `fink-broker/bin/download_tns.py`. Default is "", in
        which case the catalog will be downloaded. Beware that
        to download the catalog, you need to set environment variables:
        - TNS_API_MARKER: path to the TNS API marker (tns_marker.txt)
        - TNS_API_KEY: path to the TNS API key (tns_api.key)

    Returns
    -------
    df: DataFrame
        Spark (Streaming or SQL) DataFrame containing enriched alert data

    Examples
    --------
    >>> from fink_broker.common.spark_utils import load_parquet_files
    >>> df = load_parquet_files(rubin_alert_sample)
    >>> df = apply_science_modules(df)

    >>> classifiers_cols = df.select("clf.*").columns
    >>> assert len(classifiers_cols) == 4, classifiers_cols

    >>> xmatch_cols = df.select("xm.*").columns
    >>> assert len(xmatch_cols) == 20, xmatch_cols

    >>> prediction_cols = df.select("pred.*").columns
    >>> assert len(prediction_cols) == 5, prediction_cols

    >>> misc_cols = df.select("misc.*").columns
    >>> assert len(misc_cols) == 1, misc_cols

    >>> df.write.format("noop").mode("overwrite").save()
    """
    # Required alert columns
    # FIXME: scienceFlux?
    to_expand = ["midpointMjdTai", "band", "psfFlux", "psfFluxErr"]

    # Use for creating temp name
    prefix = "c"

    # Append temp columns with historical + current measurements
    # FIXME: ForcedSource when available?
    for colname in to_expand:
        df = concat_col(
            df,
            colname,
            prefix=prefix,
            current="diaSource",
            history="prvDiaSources",
        )
    expanded = [prefix + i for i in to_expand]

    # Apply SNAD light curve features
    ad_args = [
        "cmidpointMjdTai",
        "cpsfFlux",
        "cpsfFluxErr",
        "cband",
        "diaObject.diaObjectId",
    ]
    df = df.withColumn("lc_features", extract_features_ad(*ad_args))

    # XMATCH
    columns_pre_xmatch = df.columns  # initialise columns
    df = apply_all_xmatch(df, tns_raw_output)

    # xmatch added columns
    crossmatch_struct = [i for i in df.columns if i not in columns_pre_xmatch]

    # CLASSIFIERS
    columns_pre_classifiers = df.columns  # initialise columns

    _LOG.info("New classifier: supernnova - Ia")
    snn_args = [F.col("diaSource.diaSourceId")]
    snn_args += [F.col(i) for i in expanded]

    # Binary SN
    snn_ia_args = snn_args + [F.lit("elasticc_binary_broad/SN_vs_other")]
    df = df.withColumn("snnSnVsOthers_score", snn_ia_elasticc(*snn_ia_args))

    # CATS
    _LOG.info("New classifier: CATS")
    cats_args = ["cmidpointMjdTai", "cpsfFlux", "cpsfFluxErr", "cband"]
    df = df.withColumn("cats_broad_array_prob", predict_nn(*cats_args))

    mapping_cats_general = {
        0: 11,  # SN-like
        1: 12,  # Fast: KN, ulens, Novae, ...
        2: 13,  # Long: SLSN, TDE, PISN, ...
        3: 21,  # Periodic: RRLyrae, EB, LPV, ...
        4: 22,  # Non-periodic: AGN
    }
    mapping_cats_general_expr = F.create_map([
        F.lit(x) for x in chain(*mapping_cats_general.items())
    ])

    df = df.withColumn(
        "cats_argmax",
        F.expr(
            "array_position(cats_broad_array_prob, array_max(cats_broad_array_prob)) - 1"
        ),
    )

    # FIXME: do we want scores as well?
    df = df.withColumn("cats_class", mapping_cats_general_expr[df["cats_argmax"]])

    # _LOG.info("New classifier: SLSN (fake)")
    # slsn_args = ["diaObject.diaObjectId"]
    # slsn_args += [F.col(i) for i in expanded]
    # slsn_args += ["diaSource.ra", "diaSource.dec"]
    # df = df.withColumn("slsn_score", slsn_rubin(*slsn_args))
    # df = df.withColumn("slsn_score", F.lit(0.0))

    _LOG.info("New classifier: EarlySN Ia")
    early_ia_args = [F.col(i) for i in expanded]
    df = df.withColumn(
        "earlySNIa_score", rfscore_rainbow_elasticc_nometa(*early_ia_args)
    )

    _LOG.info("New classifier: Hostless")
    df = df.withColumn('elephant_kstest',
        run_potential_hostless(
            df["cutoutScience"],
            df["cutoutTemplate"],
            df["ssSource.ssObjectId"]
        )
    )

    # xmatch added columns
    df = df.drop(*[
        "cats_broad_array_prob",
        "cats_argmax",
    ])
    classifier_struct = [i for i in df.columns if i not in columns_pre_classifiers]

    # MISC
    columns_pre_misc = df.columns  # initialise columns
    # FIXME: this should be removed when
    # diaObject.firstDiaSourceMjdTai will be populated
    df = df.withColumn("firstDiaSourceMjdTaiFink", F.array_min("cmidpointMjdTai"))

    misc_struct = [i for i in df.columns if i not in columns_pre_misc]

    # PREDICTIONS
    columns_pre_predictor = df.columns  # initialise columns
    _LOG.info("New predictor: asteroids")
    df = df.withColumn("is_sso", df["ssSource"].isNotNull())

    _LOG.info("New predictor: first alert")
    df = df.withColumn("is_first", df["diaObject.nDiaSources"] == 1)

    _LOG.info("New predictor: cataloged")
    df = df.withColumn(
        "is_cataloged",
        (F.col("simbad_otype").isNotNull() & (F.col("simbad_otype") != "Fail"))
        | (F.col("gaiadr3_DR3Name").isNotNull() & (F.col("gaiadr3_DR3Name") != "Fail")),
    )

    # This seems redundant with `classifiers.cat_class`, but it allows
    # flexibility later to change our main classification
    _LOG.info("New predictor: Main class candidate")
    df = df.withColumn("main_label_classifier", df["cats_class"])

    # This seems redundant with `crossmatch.simbad_otype`, but it allows
    # flexibility later to change our main classification
    _LOG.info("New predictor: Main xmatch")
    df = df.withColumn("main_label_crossmatch", df["simbad_otype"])

    prediction_struct = [i for i in df.columns if i not in columns_pre_predictor]

    # Wrap into structs
    df = df.withColumn("clf", F.struct(*classifier_struct))
    df = df.drop(*classifier_struct)

    df = df.withColumn("xm", F.struct(*crossmatch_struct))
    df = df.drop(*crossmatch_struct)

    df = df.withColumn("pred", F.struct(*prediction_struct))
    df = df.drop(*prediction_struct)

    df = df.withColumn("misc", F.struct(*misc_struct))
    df = df.drop(*misc_struct)

    # Drop temp columns
    df = df.drop(*expanded)

    return df


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    globs = globals()
    root = os.environ["FINK_HOME"]
    globs["rubin_alert_sample"] = os.path.join(
        root, "datasim/rubin_test_data_10_0.parquet"
    )

    # Run the Spark test suite
    spark_unit_tests(globs)
