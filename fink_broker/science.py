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

import pandas as pd

from logging import Logger
import os

from fink_science.utilities import concat_col

from fink_science.random_forest_snia.processor import rfscore_sigmoid_full
from fink_science.xmatch.processor import cdsxmatch
from fink_science.snn.processor import snn_ia
from fink_science.microlensing.processor import mulens
from fink_science.asteroids.processor import roid_catcher
from fink_science.nalerthist.processor import nalerthist
from fink_science.kilonova.processor import knscore

from fink_broker.tester import spark_unit_tests

import numpy as np
import healpy as hp

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
    colnames = [
        df['objectId'],
        df['candidate.ra'],
        df['candidate.dec']
    ]
    df = df.withColumn('cdsxmatch', cdsxmatch(*colnames))

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

    # Drop temp columns
    df = df.drop(*expanded)

    return df


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    globs = globals()
    root = os.environ['FINK_HOME']
    globs["ztf_alert_sample"] = os.path.join(
        root, "online/raw")

    # Run the Spark test suite
    spark_unit_tests(globs)
