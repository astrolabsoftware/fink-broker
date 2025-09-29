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
from fink_science.rubin.random_forest_snia.processor import (
    rfscore_rainbow_elasticc_nometa,
)
from fink_science.rubin.snn.processor import snn_ia_elasticc
from fink_science.rubin.cats.processor import predict_nn
from fink_science.rubin.xmatch.processor import xmatch_cds
from fink_science.rubin.xmatch.processor import xmatch_tns
from fink_science.rubin.xmatch.processor import crossmatch_other_catalog
from fink_science.rubin.xmatch.processor import crossmatch_mangrove
from fink_science.rubin.xmatch.utils import MANGROVE_COLS
# from fink_science.rubin.slsn.processor import slsn_rubin

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_LOG = logging.getLogger(__name__)


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
    >>> df = apply_all_xmatch(df, tns_raw_output="")

    # apply_science_modules is lazy, so trigger the computation
    >>> an_alert = df.take(1)
    """
    alert_id = "diaSource.diaSourceId"
    ra = "diaSource.ra"
    dec = "diaSource.dec"

    _LOG.info("New xmatch: cdsxmatch")
    df = xmatch_cds(df)

    _LOG.info("New xmatch: TNS")
    df = xmatch_tns(df, tns_raw_output=tns_raw_output)

    _LOG.info("New xmatch: Gaia main xmatch (1.0 arcsec)")
    df = xmatch_cds(
        df,
        distmaxarcsec=1,
        catalogname="vizier:I/355/gaiadr3",
        cols_out=["DR3Name", "Plx", "e_Plx", "VarFlag"],
        types=["string", "float", "float", "string"],
    )

    # VarFlag is a string. Make it integer
    # 0=NOT_AVAILABLE
    # 1=VARIABLE
    df = df.withColumn(
        "vizier:I/355/gaiadr3_VarFlag",
        F.when(df["vizier:I/355/gaiadr3_VarFlag"] == "VARIABLE", 1).otherwise(0),
    )

    _LOG.info("New xmatch: Gaia var xmatch (1.0 arcsec)")
    df = xmatch_cds(
        df,
        distmaxarcsec=1,
        catalogname="vizier:I/358/vclassre",
        cols_out=["Class"],
        types=["string"],
    )

    _LOG.info("New xmatch: VSX (1.5 arcsec)")
    df = xmatch_cds(
        df,
        catalogname="vizier:B/vsx/vsx",
        distmaxarcsec=1.5,
        cols_out=["Type"],
        types=["string"],
    )

    _LOG.info("New xmatch: SPICY (1.2 arcsec)")
    df = xmatch_cds(
        df,
        catalogname="vizier:J/ApJS/254/33/table1",
        distmaxarcsec=1.2,
        cols_out=["SPICY", "class"],
        types=["int", "string"],
    )

    _LOG.info("New xmatch: GCVS (1.5 arcsec)")
    df = df.withColumn(
        "gcvs_type",
        crossmatch_other_catalog(
            df[alert_id],
            df[ra],
            df[dec],
            F.lit("gcvs"),
        ),
    )

    _LOG.info("New xmatch: 3HSP (1 arcmin)")
    df = df.withColumn(
        "x3hsp_type",
        crossmatch_other_catalog(
            df[alert_id],
            df[ra],
            df[dec],
            F.lit("3hsp"),
            F.lit(60.0),
        ),
    )

    _LOG.info("New xmatch: 4LAC (1 arcmin)")
    df = df.withColumn(
        "x4lac_type",
        crossmatch_other_catalog(
            df[alert_id],
            df[ra],
            df[dec],
            F.lit("4lac"),
            F.lit(60.0),
        ),
    )

    _LOG.info("New xmatch: Mangrove (1 acrmin)")
    df = df.withColumn(
        "mangrove",
        crossmatch_mangrove(df[alert_id], df[ra], df[dec], F.lit(60.0)),
    )

    # Explode mangrove
    for col_ in MANGROVE_COLS:
        df = df.withColumn(
            "mangrove_{}".format(col_),
            df["mangrove"].getItem(col_),
        )
    df = df.drop("mangrove")

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

    >>> classifiers_cols = df.select("classifiers.*").columns
    >>> assert len(classifiers_cols) == 17, classifiers_cols

    >>> xmatch_cols = df.select("crossmatches.*").columns
    >>> assert len(xmatch_cols) == 4, xmatch_cols

    >>> prediction_cols = df.select("predictions.*").columns
    >>> assert len(prediction_cols) == 5, prediction_cols

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

    # XMATCH
    columns_pre_xmatch = df.columns  # initialise columns
    df = apply_all_xmatch(df, tns_raw_output)

    # xmatch added columns
    crossmatch_struct = [i for i in df.columns if i not in columns_pre_xmatch]

    # CLASSIFIERS
    columns_pre_classifiers = df.columns  # initialise columns

    _LOG.info("New classifier: EarlySN Ia")
    early_ia_args = [F.col(i) for i in expanded]
    df = df.withColumn(
        "earlySNIa_score", rfscore_rainbow_elasticc_nometa(*early_ia_args)
    )

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
        0: 11,
        1: 12,
        2: 13,
        3: 21,
        4: 22,
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

    # # SLSN
    # slsn_args = ["diaObject.diaObjectId"]
    # slsn_args += [F.col(i) for i in expanded]
    # slsn_args += ["diaSource.ra", "diaSource.dec"]
    # df = df.withColumn("rf_slsn_vs_nonslsn", slsn_rubin(*slsn_args))

    # xmatch added columns
    df = df.drop(*[
        "cats_broad_array_prob",
        "cats_argmax",
    ])
    classifier_struct = [i for i in df.columns if i not in columns_pre_classifiers]

    # PREDICTIONS
    columns_pre_predictor = df.columns  # initialise columns
    _LOG.info("New predictor: asteroids")
    df = df.withColumn("is_sso", df["ssSource"].isNotNull())

    _LOG.info("New predictor: first alert")
    df = df.withColumn("is_first", F.size(df["prvDiaSources"]) == -1)

    _LOG.info("New predictor: cataloged")
    df = df.withColumn(
        "is_cataloged",
        F.col("simbad_otype").isNotNull() | F.col("vizier:I/355/gaiadr3_DR3Name").isNotNull(),
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
    df = df.withColumn("classifiers", F.struct(*classifier_struct))
    df = df.drop(*classifier_struct)

    df = df.withColumn("crossmatches", F.struct(*crossmatch_struct))
    df = df.drop(*crossmatch_struct)

    df = df.withColumn("predictions", F.struct(*prediction_struct))
    df = df.drop(*prediction_struct)

    # Drop temp columns
    df = df.drop(*expanded)

    return df


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    globs = globals()
    root = os.environ["FINK_HOME"]
    globs["rubin_alert_sample"] = os.path.join(
        root, "datasim/rubin_test_data_9_0.parquet"
    )

    # Run the Spark test suite
    spark_unit_tests(globs)
