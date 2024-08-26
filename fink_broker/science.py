# Copyright 2020-2024 AstroLab Software
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
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StringType, LongType, MapType, FloatType

import numpy as np
import pandas as pd
import healpy as hp

import os
import logging
from itertools import chain

from fink_utils.spark.utils import concat_col

from fink_broker.tester import spark_unit_tests


# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_LOG = logging.getLogger(__name__)


# Conditional import of science modules
try:
    from fink_science.random_forest_snia.processor import rfscore_sigmoid_full
    from fink_science.xmatch.processor import xmatch_cds
    from fink_science.xmatch.processor import crossmatch_other_catalog
    from fink_science.xmatch.processor import crossmatch_mangrove

    from fink_science.snn.processor import snn_ia
    from fink_science.microlensing.processor import mulens
    from fink_science.asteroids.processor import roid_catcher
    from fink_science.nalerthist.processor import nalerthist
    from fink_science.kilonova.processor import knscore
    from fink_science.ad_features.processor import extract_features_ad
    from fink_science.anomaly_detection.processor import anomaly_score

    from fink_science.random_forest_snia.processor import rfscore_rainbow_elasticc
    from fink_science.snn.processor import snn_ia_elasticc, snn_broad_elasticc
    from fink_science.cats.processor import predict_nn
    from fink_science.agn.processor import agn_elasticc
    from fink_science.slsn.processor import slsn_elasticc
    from fink_science.fast_transient_rate.processor import magnitude_rate
    from fink_science.fast_transient_rate import rate_module_output_schema
    # from fink_science.t2.processor import t2
except ImportError as e:
    _LOG.warning("Fink science modules are not available. ")
    _LOG.warning(f"exception raised: {e}")
    pass


def dec2theta(dec: float) -> float:
    """Convert Dec (deg) to theta (rad)"""
    return np.pi / 2.0 - np.pi / 180.0 * dec


def ra2phi(ra: float) -> float:
    """Convert RA (deg) to phi (rad)"""
    return np.pi / 180.0 * ra


@pandas_udf(LongType(), PandasUDFType.SCALAR)
def ang2pix(ra: pd.Series, dec: pd.Series, nside: pd.Series) -> pd.Series:
    """Compute pixel number at given nside

    Parameters
    ----------
    ra: float
        Spark column containing RA (float)
    dec: float
        Spark column containing RA (float)
    nside: int
        Spark column containing nside

    Returns
    -------
    out: long
        Spark column containing pixel number

    Examples
    --------
    >>> from fink_broker.spark_utils import load_parquet_files
    >>> df = load_parquet_files(ztf_alert_sample)

    >>> df_index = df.withColumn(
    ...     'p',
    ...     ang2pix(df['candidate.ra'], df['candidate.dec'], F.lit(256))
    ... )
    >>> df_index.select('p').take(1)[0][0] > 0
    True
    """
    return pd.Series(
        hp.ang2pix(
            nside.to_numpy()[0], dec2theta(dec.to_numpy()), ra2phi(ra.to_numpy())
        )
    )


@pandas_udf(StringType(), PandasUDFType.SCALAR)
def ang2pix_array(ra: pd.Series, dec: pd.Series, nside: pd.Series) -> pd.Series:
    """Return a col string with the pixel numbers corresponding to the nsides

    pix@nside[0]_pix@nside[1]_...etc

    Parameters
    ----------
    ra: float
        Spark column containing RA (float)
    dec: float
        Spark column containing RA (float)
    nside: list
        Spark column containing list of nside

    Returns
    -------
    out: str
        Spark column containing _ separated pixel values

    Examples
    --------
    >>> from fink_broker.spark_utils import load_parquet_files
    >>> df = load_parquet_files(ztf_alert_sample)

    >>> nsides = F.array([F.lit(256), F.lit(4096), F.lit(131072)])
    >>> df_index = df.withColumn(
    ...     'p',
    ...     ang2pix_array(df['candidate.ra'], df['candidate.dec'], nsides)
    ... )
    >>> l = len(df_index.select('p').take(1)[0][0].split('_'))
    >>> print(l)
    3
    """
    pixs = [
        hp.ang2pix(int(nside_), dec2theta(dec.to_numpy()), ra2phi(ra.to_numpy()))
        for nside_ in nside.to_numpy()[0]
    ]

    to_return = ["_".join(list(np.array(i, dtype=str))) for i in np.transpose(pixs)]

    return pd.Series(to_return)


@pandas_udf(MapType(StringType(), FloatType()), PandasUDFType.SCALAR)
def fake_t2(incol):
    """Return all t2 probabilities as zero

    Only for test purposes.
    """
    keys = [
        "M-dwarf",
        "KN",
        "AGN",
        "SLSN-I",
        "RRL",
        "Mira",
        "SNIax",
        "TDE",
        "SNIa",
        "SNIbc",
        "SNIa-91bg",
        "mu-Lens-Single",
        "EB",
        "SNII",
    ]
    values = [0.0] * len(keys)
    out = {k: v for k, v in zip(keys, values)}  # noqa: C416
    return pd.Series([out] * len(incol))


def apply_science_modules(df: DataFrame, noscience: bool = False) -> DataFrame:
    """Load and apply Fink science modules to enrich alert content

    Focus on ZTF stream

    Parameters
    ----------
    df: DataFrame
        Spark (Streaming or SQL) DataFrame containing raw alert data

    Returns
    -------
    df: DataFrame
        Spark (Streaming or SQL) DataFrame containing enriched alert data
    noscience: bool
        Return untouched input dataframe, useful for Fink-broker infrastructure validation

    Examples
    --------
    >>> from fink_broker.spark_utils import load_parquet_files
    >>> from fink_broker.logging_utils import get_fink_logger
    >>> logger = get_fink_logger('raw2cience_test', 'INFO')
    >>> _LOG = logging.getLogger(__name__)
    >>> df = load_parquet_files(ztf_alert_sample)
    >>> df = apply_science_modules(df)

    # apply_science_modules is lazy, so trigger the computation
    >>> an_alert = df.take(1)
    """
    if noscience:
        return df

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

    # Apply level one processor: cdsxmatch
    _LOG.info("New processor: cdsxmatch")
    df = xmatch_cds(df)

    _LOG.info("New processor: Gaia xmatch (1.0 arcsec)")
    df = xmatch_cds(
        df,
        distmaxarcsec=1,
        catalogname="vizier:I/355/gaiadr3",
        cols_out=["DR3Name", "Plx", "e_Plx"],
        types=["string", "float", "float"],
    )

    _LOG.info("New processor: VSX (1.5 arcsec)")
    df = xmatch_cds(
        df,
        catalogname="vizier:B/vsx/vsx",
        distmaxarcsec=1.5,
        cols_out=["Type"],
        types=["string"],
    )
    # legacy -- rename `Type` into `vsx`
    # see https://github.com/astrolabsoftware/fink-broker/issues/787
    df = df.withColumnRenamed("Type", "vsx")

    _LOG.info("New processor: SPICY (1.2 arcsec)")
    df = xmatch_cds(
        df,
        catalogname="vizier:J/ApJS/254/33/table1",
        distmaxarcsec=1.2,
        cols_out=["SPICY", "class"],
        types=["int", "string"],
    )
    # rename `SPICY` into `spicy_id`. Values are number or null
    df = df.withColumnRenamed("SPICY", "spicy_id")
    # Cast null into -1
    df = df.withColumn(
        "spicy_id", F.when(df["spicy_id"].isNull(), F.lit(-1)).otherwise(df["spicy_id"])
    )

    # rename `class` into `spicy_class`. Values are:
    # Unknown, FS, ClassI, ClassII, ClassIII, or 'nan'
    df = df.withColumnRenamed("class", "spicy_class")
    # Make 'nan' 'Unknown'
    df = df.withColumn(
        "spicy_class",
        F.when(df["spicy_class"] == "nan", F.lit("Unknown")).otherwise(
            df["spicy_class"]
        ),
    )

    _LOG.info("New processor: GCVS (1.5 arcsec)")
    df = df.withColumn(
        "gcvs",
        crossmatch_other_catalog(
            df["candidate.candid"],
            df["candidate.ra"],
            df["candidate.dec"],
            F.lit("gcvs"),
        ),
    )

    _LOG.info("New processor: 3HSP (1 arcmin)")
    df = df.withColumn(
        "x3hsp",
        crossmatch_other_catalog(
            df["candidate.candid"],
            df["candidate.ra"],
            df["candidate.dec"],
            F.lit("3hsp"),
            F.lit(60.0),
        ),
    )

    _LOG.info("New processor: 4LAC (1 arcmin)")
    df = df.withColumn(
        "x4lac",
        crossmatch_other_catalog(
            df["candidate.candid"],
            df["candidate.ra"],
            df["candidate.dec"],
            F.lit("4lac"),
            F.lit(60.0),
        ),
    )

    _LOG.info("New processor: Mangrove (1 acrmin)")
    df = df.withColumn(
        "mangrove",
        crossmatch_mangrove(
            df["candidate.candid"], df["candidate.ra"], df["candidate.dec"], F.lit(60.0)
        ),
    )

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

    _LOG.info("New processor: T2")
    # t2_args = ['candid', 'cjd', 'cfid', 'cmagpsf', 'csigmapsf']
    # t2_args += [F.col('roid'), F.col('cdsxmatch'), F.col('candidate.jdstarthist')]
    # df = df.withColumn('t2', t2(*t2_args))
    df = df.withColumn("t2", fake_t2("objectId"))

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
    df = df.withColumn("anomaly_score", anomaly_score("lc_features"))

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

    # Drop temp columns
    df = df.drop(*expanded)

    return df


def apply_science_modules_elasticc(df: DataFrame) -> DataFrame:
    """Load and apply Fink science modules to enrich alert content

    Currently available:
    - AGN
    - CBPF (broad)
    - SNN (Ia, and broad)

    Focus on ELAsTICC stream

    Parameters
    ----------
    df: DataFrame
        Spark (Streaming or SQL) DataFrame containing raw alert data

    Returns
    -------
    df: DataFrame
        Spark (Streaming or SQL) DataFrame containing enriched alert data

    Examples
    --------
    >>> from fink_broker.spark_utils import load_parquet_files
    >>> from fink_broker.logging_utils import get_fink_logger
    >>> logger = get_fink_logger('raw2cience_elasticc_test', 'INFO')
    >>> _LOG = logging.getLogger(__name__)
    >>> df = load_parquet_files(elasticc_alert_sample)
    >>> df = apply_science_modules_elasticc(df)
    """
    # Required alert columns
    to_expand = ["midPointTai", "filterName", "psFlux", "psFluxErr"]

    # Use for creating temp name
    prefix = "c"

    # Append temp columns with historical + current measurements
    for colname in to_expand:
        df = concat_col(
            df,
            colname,
            prefix=prefix,
            current="diaSource",
            history="prvDiaForcedSources",
        )
    expanded = [prefix + i for i in to_expand]

    _LOG.info("New processor: xmatch (random positions)")
    # Assuming random positions
    df = df.withColumn("cdsxmatch", F.lit("Unknown"))

    _LOG.info("New processor: asteroids (random positions)")
    df = df.withColumn("roid", F.lit(0))

    # add redshift
    df = df.withColumn("redshift", F.col("diaObject.z_final"))
    df = df.withColumn("redshift_err", F.col("diaObject.z_final_err"))

    _LOG.info("New processor: EarlySN")
    args = ["cmidPointTai", "cfilterName", "cpsFlux", "cpsFluxErr"]
    args += [F.col("diaSource.snr")]
    args += [F.col("diaObject.hostgal_snsep")]
    args += [F.col("diaObject.hostgal_zphot")]

    df = df.withColumn("rf_snia_vs_nonia", rfscore_rainbow_elasticc(*args))

    # Apply level one processor: superNNova
    _LOG.info("New processor: supernnova - Ia")
    args = [F.col("diaSource.diaSourceId")]
    args += [
        F.col("cmidPointTai"),
        F.col("cfilterName"),
        F.col("cpsFlux"),
        F.col("cpsFluxErr"),
    ]
    args += [F.col("roid"), F.col("cdsxmatch"), F.array_min("cmidPointTai")]
    args += [F.col("diaObject.mwebv"), F.col("redshift"), F.col("redshift_err")]

    # Binary Ia
    full_args = args + [F.lit("elasticc_ia")]
    df = df.withColumn("snn_snia_vs_nonia", snn_ia_elasticc(*full_args))

    # Binary SN
    full_args = args + [F.lit("elasticc_binary_broad/SN_vs_other")]
    df = df.withColumn("snn_sn_vs_others", snn_ia_elasticc(*full_args))

    # Binary Periodic
    full_args = args + [F.lit("elasticc_binary_broad/Periodic_vs_other")]
    df = df.withColumn("snn_periodic_vs_others", snn_ia_elasticc(*full_args))

    # Binary nonperiodic
    full_args = args + [F.lit("elasticc_binary_broad/NonPeriodic_vs_other")]
    df = df.withColumn("snn_nonperiodic_vs_others", snn_ia_elasticc(*full_args))

    # Binary Long
    full_args = args + [F.lit("elasticc_binary_broad/Long_vs_other")]
    df = df.withColumn("snn_long_vs_others", snn_ia_elasticc(*full_args))

    # Binary Fast
    full_args = args + [F.lit("elasticc_binary_broad/Fast_vs_other")]
    df = df.withColumn("snn_fast_vs_others", snn_ia_elasticc(*full_args))

    _LOG.info("New processor: supernnova - Broad")
    args = [F.col("diaSource.diaSourceId")]
    args += [
        F.col("cmidPointTai"),
        F.col("cfilterName"),
        F.col("cpsFlux"),
        F.col("cpsFluxErr"),
    ]
    args += [F.col("roid"), F.col("cdsxmatch"), F.array_min("cmidPointTai")]
    args += [F.col("diaObject.mwebv"), F.col("redshift"), F.col("redshift_err")]
    args += [F.lit("elasticc_broad")]
    df = df.withColumn("preds_snn", snn_broad_elasticc(*args))

    mapping_snn = {
        0: 11,
        1: 13,
        2: 12,
        3: 22,
        4: 21,
    }
    mapping_snn_expr = F.create_map([F.lit(x) for x in chain(*mapping_snn.items())])

    df = df.withColumn(
        "snn_argmax", F.expr("array_position(preds_snn, array_max(preds_snn)) - 1")
    )
    df = df.withColumn("snn_broad_class", mapping_snn_expr[df["snn_argmax"]])
    df = df.withColumnRenamed("preds_snn", "snn_broad_array_prob")

    # CBPF
    args = ["cmidPointTai", "cpsFlux", "cpsFluxErr", "cfilterName"]
    args += [
        F.col("diaObject.mwebv"),
        F.col("diaObject.z_final"),
        F.col("diaObject.z_final_err"),
    ]
    args += [F.col("diaObject.hostgal_zphot"), F.col("diaObject.hostgal_zphot_err")]
    df = df.withColumn("cbpf_preds", predict_nn(*args))

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
        "cats_argmax", F.expr("array_position(cbpf_preds, array_max(cbpf_preds)) - 1")
    )
    df = df.withColumn("cats_broad_class", mapping_cats_general_expr[df["cats_argmax"]])
    df = df.withColumnRenamed("cbpf_preds", "cats_broad_array_prob")

    # AGN & SLSN
    args_forced = [
        "diaObject.diaObjectId",
        "cmidPointTai",
        "cpsFlux",
        "cpsFluxErr",
        "cfilterName",
        "diaSource.ra",
        "diaSource.decl",
        "diaObject.hostgal_zphot",
        "diaObject.hostgal_zphot_err",
        "diaObject.hostgal_ra",
        "diaObject.hostgal_dec",
    ]
    df = df.withColumn("rf_agn_vs_nonagn", agn_elasticc(*args_forced))
    df = df.withColumn("rf_slsn_vs_nonslsn", slsn_elasticc(*args_forced))

    # Drop temp columns
    df = df.drop(*expanded)
    df = df.drop(*[
        "preds_snn",
        "cbpf_preds",
        "redshift",
        "redshift_err",
        "cdsxmatch",
        "roid",
        "cats_argmax",
        "snn_argmax",
    ])

    return df


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    globs = globals()
    root = os.environ["FINK_HOME"]
    globs["ztf_alert_sample"] = os.path.join(root, "online/raw/20200101")

    globs["elasticc_alert_sample"] = os.path.join(root, "datasim/elasticc_alerts")

    # Run the Spark test suite
    spark_unit_tests(globs)
