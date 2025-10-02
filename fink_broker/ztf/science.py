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

from fink_utils.spark.utils import concat_col

from fink_broker.common.tester import spark_unit_tests

# Import of science modules
from fink_broker.common.science import apply_all_xmatch

from fink_science.ztf.random_forest_snia.processor import rfscore_sigmoid_full

from fink_science.ztf.snn.processor import snn_ia
from fink_science.ztf.microlensing.processor import mulens
from fink_science.ztf.asteroids.processor import roid_catcher
from fink_science.ztf.nalerthist.processor import nalerthist
from fink_science.ztf.kilonova.processor import knscore
from fink_science.ztf.ad_features.processor import extract_features_ad
from fink_science.ztf.anomaly_detection.processor import anomaly_score
from fink_science.ztf.anomaly_detection.processor import ANOMALY_MODELS
from fink_science.ztf.transient_features.processor import extract_transient_features

from fink_science.ztf.fast_transient_rate.processor import magnitude_rate
from fink_science.ztf.fast_transient_rate import rate_module_output_schema

from fink_science.ztf.blazar_low_state.processor import quiescent_state
from fink_science.ztf.standardized_flux.processor import standardized_flux

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_LOG = logging.getLogger(__name__)


def apply_science_modules(df: DataFrame, tns_raw_output: str = "") -> DataFrame:
    """Load and apply Fink science modules to enrich alert content

    Focus on ZTF stream

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
    >>> from fink_broker.common.logging_utils import get_fink_logger
    >>> logger = get_fink_logger('raw2cience_test', 'INFO')
    >>> _LOG = logging.getLogger(__name__)
    >>> df = load_parquet_files(ztf_alert_sample)
    >>> df = apply_science_modules(df)

    # apply_science_modules is lazy, so trigger the computation
    >>> an_alert = df.take(1)
    """
    # Retrieve time-series information
    to_expand = [
        "jd",
        "fid",
        "magpsf",
        "sigmapsf",
        "magnr",
        "sigmagnr",
        "isdiffpos",
        "distnr",
        "diffmaglim",
    ]

    # Append temp columns with historical + current measurements
    prefix = "c"
    for colname in to_expand:
        df = concat_col(df, colname, prefix=prefix)
    expanded = [prefix + i for i in to_expand]

    df = apply_all_xmatch(df, tns_raw_output, survey="ztf")

    # Apply level one processor: asteroids
    _LOG.info("New processor: asteroids")
    args_roid = [
        "cjd",
        "cmagpsf",
        "candidate.ndethist",
        "candidate.sgscore1",
        "candidate.ssdistnr",
        "candidate.distpsnr1",
    ]
    df = df.withColumn("roid", roid_catcher(*args_roid))

    _LOG.info("New processor: Active Learning")

    # Perform the fit + classification.
    # Note we can omit the model_path argument, and in that case the
    # default model `data/models/default-model.obj` will be used.
    rfscore_args = ["cjd", "cfid", "cmagpsf", "csigmapsf"]
    rfscore_args += [F.col("cdsxmatch"), F.col("candidate.ndethist")]
    df = df.withColumn("rf_snia_vs_nonia", rfscore_sigmoid_full(*rfscore_args))

    # Apply level one processor: superNNova
    _LOG.info("New processor: supernnova")

    snn_args = ["candid", "cjd", "cfid", "cmagpsf", "csigmapsf"]
    snn_args += [F.col("roid"), F.col("cdsxmatch"), F.col("candidate.jdstarthist")]
    snn_args += [F.lit("snn_snia_vs_nonia")]
    df = df.withColumn("snn_snia_vs_nonia", snn_ia(*snn_args))

    snn_args = ["candid", "cjd", "cfid", "cmagpsf", "csigmapsf"]
    snn_args += [F.col("roid"), F.col("cdsxmatch"), F.col("candidate.jdstarthist")]
    snn_args += [F.lit("snn_sn_vs_all")]
    df = df.withColumn("snn_sn_vs_all", snn_ia(*snn_args))

    # Apply level one processor: microlensing
    _LOG.info("New processor: microlensing")

    # Required alert columns - already computed for SN
    mulens_args = [
        "cfid",
        "cmagpsf",
        "csigmapsf",
        "cmagnr",
        "csigmagnr",
        "cisdiffpos",
        "candidate.ndethist",
    ]
    df = df.withColumn("mulens", mulens(*mulens_args))

    # Apply level one processor: nalerthist
    _LOG.info("New processor: nalerthist")
    df = df.withColumn("nalerthist", nalerthist(df["cmagpsf"]))

    # Apply level one processor: kilonova detection
    _LOG.info("New processor: kilonova")
    knscore_args = ["cjd", "cfid", "cmagpsf", "csigmapsf"]
    knscore_args += [
        F.col("candidate.jdstarthist"),
        F.col("cdsxmatch"),
        F.col("candidate.ndethist"),
    ]
    df = df.withColumn("rf_kn_vs_nonkn", knscore(*knscore_args))

    # Apply level one processor: snad (light curve features)
    _LOG.info("New processor: ad_features")
    ad_args = [
        "cmagpsf",
        "cjd",
        "csigmapsf",
        "cfid",
        "objectId",
        "cdistnr",
        "cmagnr",
        "csigmagnr",
        "cisdiffpos",
    ]

    df = df.withColumn("lc_features", extract_features_ad(*ad_args))
    # Apply level one processor: anomaly_score
    _LOG.info("New processor: Anomaly score")
    LIST_OF_MODELS = [""] + ANOMALY_MODELS  # '' - model for a public channel
    for model in LIST_OF_MODELS:
        _LOG.info(f"...Anomaly score{model}")
        df = df.withColumn(
            f"anomaly_score{model}", anomaly_score("lc_features", F.lit(model))
        )

    # split features
    df = (
        df.withColumn("lc_features_g", df["lc_features"].getItem("1"))
        .withColumn("lc_features_r", df["lc_features"].getItem("2"))
        .drop("lc_features")
    )

    # Apply level one processor: fast transient
    _LOG.info("New processor: magnitude rate for fast transient")
    mag_rate_args = [
        "candidate.magpsf",
        "candidate.sigmapsf",
        "candidate.jd",
        "candidate.jdstarthist",
        "candidate.fid",
        "cmagpsf",
        "csigmapsf",
        "cjd",
        "cfid",
        "cdiffmaglim",
        F.lit(10000),
        F.lit(None),
    ]
    cols_before = df.columns
    df = df.withColumn("ft_module", magnitude_rate(*mag_rate_args))
    df = df.select(
        cols_before
        + [df["ft_module"][k].alias(k) for k in rate_module_output_schema.keys()]
    )

    _LOG.info("New processor: flux standardisation for blazars")
    standardisation_args = [
        "candid",
        "objectId",
        "cdistnr",
        "cmagpsf",
        "csigmapsf",
        "cmagnr",
        "csigmagnr",
        "cisdiffpos",
        "cfid",
        "cjd",
    ]
    df = df.withColumn("container", standardized_flux(*standardisation_args))

    _LOG.info("New processor: blazars low state detection")
    blazar_args = ["candid", "objectId", F.col("container").getItem("flux"), "cjd"]
    df = df.withColumn("blazar_stats", quiescent_state(*blazar_args))

    # Clean temporary container
    df = df.drop("container")

    _LOG.info("New processor: transient features")
    cols_before = df.columns
    df = extract_transient_features(df)
    extra_cols = [i for i in df.columns if i not in cols_before]

    df = df.withColumn(
        "is_transient",
        ~df["faint"]
        & df["positivesubtraction"]
        & df["real"]
        & ~df["pointunderneath"]
        & ~df["brightstar"]
        & ~df["variablesource"]
        & df["stationary"]
        & (F.col("roid") == 0),
    )

    # Drop intermediate columns
    df = df.drop(*extra_cols)

    # Drop temp columns
    df = df.drop(*expanded)

    return df


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    globs = globals()
    root = os.environ["FINK_HOME"]
    globs["ztf_alert_sample"] = os.path.join(root, "online/raw/20200101")

    # Run the Spark test suite
    spark_unit_tests(globs)
