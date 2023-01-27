# Copyright 2020-2022 AstroLab Software
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
from pyspark.sql.types import StringType, LongType

import numpy as np
import pandas as pd
import healpy as hp

import os
from logging import Logger
from itertools import chain

from fink_utils.spark.utils import concat_col

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

from fink_science.random_forest_snia.processor import rfscore_sigmoid_elasticc
from fink_science.snn.processor import snn_ia_elasticc, snn_broad_elasticc
from fink_science.cats.processor import predict_nn
from fink_science.agn.processor import agn_elasticc
from fink_science.t2.processor import t2

from fink_broker.tester import spark_unit_tests

def dec2theta(dec: float) -> float:
    """ Convert Dec (deg) to theta (rad)
    """
    return np.pi / 2.0 - np.pi / 180.0 * dec

def ra2phi(ra: float) -> float:
    """ Convert RA (deg) to phi (rad)
    """
    return np.pi / 180.0 * ra

@pandas_udf(LongType(), PandasUDFType.SCALAR)
def ang2pix(ra: pd.Series, dec: pd.Series, nside: pd.Series) -> pd.Series:
    """ Compute pixel number at given nside

    Parameters
    ----------
    ra: float
        Spark column containing RA (float)
    dec: float
        Spark column containing RA (float)
    nside: int
        Spark column containing nside

    Returns
    ----------
    out: long
        Spark column containing pixel number

    Examples
    ----------
    >>> from fink_broker.sparkUtils import load_parquet_files
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
            nside.values[0],
            dec2theta(dec.values),
            ra2phi(ra.values)
        )
    )

@pandas_udf(StringType(), PandasUDFType.SCALAR)
def ang2pix_array(ra: pd.Series, dec: pd.Series, nside: pd.Series) -> pd.Series:
    """ Return a col string with the pixel numbers corresponding to the nsides

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
    ----------
    out: str
        Spark column containing _ separated pixel values

    Examples
    ----------
    >>> from fink_broker.sparkUtils import load_parquet_files
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
        hp.ang2pix(
            int(nside_),
            dec2theta(dec.values),
            ra2phi(ra.values)
        ) for nside_ in nside.values[0]
    ]

    to_return = [
        '_'.join(list(np.array(i, dtype=str))) for i in np.transpose(pixs)
    ]

    return pd.Series(to_return)

def apply_science_modules(df: DataFrame, logger: Logger) -> DataFrame:
    """Load and apply Fink science modules to enrich alert content

    Focus on ZTF stream

    Parameters
    ----------
    df: DataFrame
        Spark (Streaming or SQL) DataFrame containing raw alert data
    logger: Logger
        Fink logger

    Returns
    ----------
    df: DataFrame
        Spark (Streaming or SQL) DataFrame containing enriched alert data

    Examples
    ----------
    >>> from fink_broker.sparkUtils import load_parquet_files
    >>> from fink_broker.loggingUtils import get_fink_logger
    >>> logger = get_fink_logger('raw2cience_test', 'INFO')
    >>> df = load_parquet_files(ztf_alert_sample)
    >>> df = apply_science_modules(df, logger)

    # apply_science_modules is lazy, so trigger the computation
    >>> an_alert = df.take(1)
    """
    # Retrieve time-series information
    to_expand = [
        'jd', 'fid', 'magpsf', 'sigmapsf',
        'magnr', 'sigmagnr', 'magzpsci', 'isdiffpos'
    ]

    # Append temp columns with historical + current measurements
    prefix = 'c'
    for colname in to_expand:
        df = concat_col(df, colname, prefix=prefix)
    expanded = [prefix + i for i in to_expand]

    # Apply level one processor: cdsxmatch
    logger.info("New processor: cdsxmatch")
    df = xmatch_cds(df)

    logger.info("New processor: Gaia xmatch")
    df = xmatch_cds(
        df,
        distmaxarcsec=1,
        catalogname='vizier:I/355/gaiadr3',
        cols_out=['DR3Name', 'Plx', 'e_Plx'],
        types=['string', 'float', 'float']
    )

    logger.info("New processor: GCVS")
    df = df.withColumn(
        'gcvs',
        crossmatch_other_catalog(
            df['candidate.candid'],
            df['candidate.ra'],
            df['candidate.dec'],
            F.lit('gcvs')
        )
    )

    logger.info("New processor: VSX")
    df = df.withColumn(
        'vsx',
        crossmatch_other_catalog(
            df['candidate.candid'],
            df['candidate.ra'],
            df['candidate.dec'],
            F.lit('vsx')
        )
    )

    logger.info("New processor: 3HSP (1 arcmin)")
    df = df.withColumn(
        'x3hsp',
        crossmatch_other_catalog(
            df['candidate.candid'],
            df['candidate.ra'],
            df['candidate.dec'],
            F.lit('3hsp'),
            F.lit(60.0)
        )
    )

    logger.info("New processor: 4LAC (1 arcmin)")
    df = df.withColumn(
        'x4lac',
        crossmatch_other_catalog(
            df['candidate.candid'],
            df['candidate.ra'],
            df['candidate.dec'],
            F.lit('4lac'),
            F.lit(60.0)
        )
    )

    logger.info("New processor: Mangrove (1 acrmin)")
    df = df.withColumn(
        'mangrove',
        crossmatch_mangrove(
            df['candidate.candid'],
            df['candidate.ra'],
            df['candidate.dec'],
            F.lit(60.0)
        )
    )

    # Apply level one processor: asteroids
    logger.info("New processor: asteroids")
    args_roid = [
        'cjd', 'cmagpsf',
        'candidate.ndethist', 'candidate.sgscore1',
        'candidate.ssdistnr', 'candidate.distpsnr1']
    df = df.withColumn('roid', roid_catcher(*args_roid))

    logger.info("New processor: Active Learning")

    # Perform the fit + classification.
    # Note we can omit the model_path argument, and in that case the
    # default model `data/models/default-model.obj` will be used.
    rfscore_args = ['cjd', 'cfid', 'cmagpsf', 'csigmapsf']
    rfscore_args += [F.col('cdsxmatch'), F.col('candidate.ndethist')]
    df = df.withColumn(
        'rf_snia_vs_nonia',
        rfscore_sigmoid_full(*rfscore_args)
    )

    # Apply level one processor: superNNova
    logger.info("New processor: supernnova")

    snn_args = ['candid', 'cjd', 'cfid', 'cmagpsf', 'csigmapsf']
    snn_args += [
        F.col('roid'), F.col('cdsxmatch'), F.col('candidate.jdstarthist')
    ]
    snn_args += [F.lit('snn_snia_vs_nonia')]
    df = df.withColumn('snn_snia_vs_nonia', snn_ia(*snn_args))

    snn_args = ['candid', 'cjd', 'cfid', 'cmagpsf', 'csigmapsf']
    snn_args += [
        F.col('roid'), F.col('cdsxmatch'), F.col('candidate.jdstarthist')
    ]
    snn_args += [F.lit('snn_sn_vs_all')]
    df = df.withColumn('snn_sn_vs_all', snn_ia(*snn_args))

    # Apply level one processor: microlensing
    logger.info("New processor: microlensing")

    # Required alert columns - already computed for SN
    mulens_args = [
        'cfid', 'cmagpsf', 'csigmapsf',
        'cmagnr', 'csigmagnr', 'cmagzpsci',
        'cisdiffpos', 'candidate.ndethist'
    ]
    df = df.withColumn('mulens', mulens(*mulens_args))

    # Apply level one processor: nalerthist
    logger.info("New processor: nalerthist")
    df = df.withColumn('nalerthist', nalerthist(df['cmagpsf']))

    # Apply level one processor: kilonova detection
    logger.info("New processor: kilonova")
    knscore_args = ['cjd', 'cfid', 'cmagpsf', 'csigmapsf']
    knscore_args += [
        F.col('candidate.jdstarthist'),
        F.col('cdsxmatch'),
        F.col('candidate.ndethist')
    ]
    df = df.withColumn('rf_kn_vs_nonkn', knscore(*knscore_args))

    logger.info("New processor: T2")
    t2_args = ['candid', 'cjd', 'cfid', 'cmagpsf', 'csigmapsf']
    t2_args += [F.col('roid'), F.col('cdsxmatch'), F.col('candidate.jdstarthist')]
    df = df.withColumn('t2', t2(*t2_args))

    # Apply level one processor: snad (light curve features)
    logger.info("New processor: ad_features")
    ad_args = ['cmagpsf', 'cjd', 'csigmapsf', 'cfid', 'objectId']
    df = df.withColumn('lc_features', extract_features_ad(*ad_args))

    # Apply level one processor: anomaly_score
    logger.info("New processor: Anomaly score")
    df = df.withColumn('anomaly_score', anomaly_score('lc_features'))

    # split features
    df = df.withColumn("lc_features_g", df['lc_features'].getItem("1"))\
        .withColumn("lc_features_r", df['lc_features'].getItem("2"))\
        .drop('lc_features')

    # Drop temp columns
    df = df.drop(*expanded)

    return df

def apply_science_modules_elasticc(df: DataFrame, logger: Logger) -> DataFrame:
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
    logger: Logger
        Fink logger

    Returns
    ----------
    df: DataFrame
        Spark (Streaming or SQL) DataFrame containing enriched alert data

    Examples
    ----------
    >>> from fink_broker.sparkUtils import load_parquet_files
    >>> from fink_broker.loggingUtils import get_fink_logger
    >>> logger = get_fink_logger('raw2cience_elasticc_test', 'INFO')
    >>> df = load_parquet_files(elasticc_alert_sample)
    >>> df = apply_science_modules_elasticc(df, logger)
    """
    # Required alert columns
    to_expand = ['midPointTai', 'filterName', 'psFlux', 'psFluxErr']

    # Use for creating temp name
    prefix = 'c'

    # Append temp columns with historical + current measurements
    for colname in to_expand:
        df = concat_col(
            df, colname, prefix=prefix,
            current='diaSource', history='prvDiaForcedSources'
        )
    expanded = [prefix + i for i in to_expand]

    logger.info("New processor: xmatch (random positions)")
    # Assuming random positions
    df = df.withColumn('cdsxmatch', F.lit('Unknown'))

    logger.info("New processor: asteroids (random positions)")
    df = df.withColumn('roid', F.lit(0))

    # add redshift
    df = df.withColumn('redshift', F.col('diaObject.z_final'))
    df = df.withColumn('redshift_err', F.col('diaObject.z_final_err'))

    logger.info("New processor: EarlySN")
    args = ['cmidPointTai', 'cfilterName', 'cpsFlux', 'cpsFluxErr']
    # fake args
    args += [F.col('cdsxmatch'), F.lit(20), F.lit(40)]
    df = df.withColumn('rf_snia_vs_nonia', rfscore_sigmoid_elasticc(*args))

    # Apply level one processor: superNNova
    logger.info("New processor: supernnova - Ia")
    args = [F.col('diaSource.diaSourceId')]
    args += [F.col('cmidPointTai'), F.col('cfilterName'), F.col('cpsFlux'), F.col('cpsFluxErr')]
    args += [F.col('roid'), F.col('cdsxmatch'), F.array_min('cmidPointTai')]
    args += [F.col('diaObject.mwebv'), F.col('redshift'), F.col('redshift_err')]
    args += [F.lit('elasticc_ia')]
    df = df.withColumn('snn_snia_vs_nonia', snn_ia_elasticc(*args))

    logger.info("New processor: supernnova - Broad")
    args = [F.col('diaSource.diaSourceId')]
    args += [F.col('cmidPointTai'), F.col('cfilterName'), F.col('cpsFlux'), F.col('cpsFluxErr')]
    args += [F.col('roid'), F.col('cdsxmatch'), F.array_min('cmidPointTai')]
    args += [F.col('diaObject.mwebv'), F.col('redshift'), F.col('redshift_err')]
    args += [F.lit('elasticc_broad')]
    df = df.withColumn('preds_snn', snn_broad_elasticc(*args))

    mapping_snn = {
        -1: 0,
        0: 11,
        1: 13,
        2: 12,
        3: 22,
        4: 21,
    }
    mapping_snn_expr = F.create_map([F.lit(x) for x in chain(*mapping_snn.items())])

    col_class = F.col('preds_snn').getItem(0).astype('int')
    df = df.withColumn('snn_broad_class', mapping_snn_expr[col_class])
    df = df.withColumn('snn_broad_max_prob', F.col('preds_snn').getItem(1))

    # CBPF
    args = ['cmidPointTai', 'cpsFlux', 'cpsFluxErr', 'cfilterName']
    args += [F.col('diaObject.mwebv'), F.col('diaObject.z_final'), F.col('diaObject.z_final_err')]
    args += [F.col('diaObject.hostgal_zphot'), F.col('diaObject.hostgal_zphot_err')]
    df = df.withColumn('cbpf_preds', predict_nn(*args))

    mapping_cats_general = {
        -1: 0,
        0: 111,
        1: 112,
        2: 113,
        3: 114,
        4: 115,
        5: 121,
        6: 122,
        7: 123,
        8: 124,
        9: 131,
        10: 132,
        11: 133,
        12: 134,
        13: 135,
        14: 211,
        15: 212,
        16: 213,
        17: 214,
        18: 221
    }
    mapping_cats_general_expr = F.create_map([F.lit(x) for x in chain(*mapping_cats_general.items())])

    col_fine_class = F.col('cbpf_preds').getItem(0).astype('int')
    df = df.withColumn('cats_fine_class', mapping_cats_general_expr[col_fine_class])
    df = df.withColumn('cats_fine_max_prob', F.col('cbpf_preds').getItem(1))

    # AGN
    args_forced = [
        'diaObject.diaObjectId', 'cmidPointTai', 'cpsFlux', 'cpsFluxErr', 'cfilterName',
        'diaSource.ra', 'diaSource.decl',
        'diaObject.hostgal_zphot', 'diaObject.hostgal_zphot_err',
        'diaObject.hostgal_ra', 'diaObject.hostgal_dec'
    ]
    df = df.withColumn('rf_agn_vs_nonagn', agn_elasticc(*args_forced))

    # T2
    df = df.withColumn('t2_broad_class', F.lit(0))
    df = df.withColumn('t2_broad_max_prob', F.lit(0.0))

    # Drop temp columns
    df = df.drop(*expanded)
    df = df.drop(*['preds_snn', 'cbpf_preds', 'redshift', 'redshift_err', 'cdsxmatch', 'roid'])

    return df


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    globs = globals()
    root = os.environ['FINK_HOME']
    globs["ztf_alert_sample"] = os.path.join(
        root, "online/raw")

    globs['elasticc_alert_sample'] = os.path.join(
        root, "datasim/elasticc_alerts")

    # Run the Spark test suite
    spark_unit_tests(globs)
