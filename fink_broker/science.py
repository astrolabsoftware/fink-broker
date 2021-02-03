# Copyright 2020 AstroLab Software
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
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StringType, LongType

import pandas as pd

from logging import Logger
import os

from fink_science.utilities import concat_col

from fink_science import __file__ as fspath
from fink_science.random_forest_snia.processor import rfscore_sigmoid_full
from fink_science.xmatch.processor import cdsxmatch
from fink_science.snn.processor import snn_ia
from fink_science.microlensing.processor import mulens
from fink_science.microlensing.classifier import load_mulens_schema_twobands
from fink_science.microlensing.classifier import load_external_model
from fink_science.asteroids.processor import roid_catcher
from fink_science.nalerthist.processor import nalerthist

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
def ang2pix(ra, dec, nside):
    """
    """
    return pd.Series(
        hp.ang2pix(
            nside.values[0],
            dec2theta(dec.values),
            ra2phi(ra.values)
        )
    )

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

    # Apply level one processor: rfscore
    logger.info("New processor: rfscore")

    # Perform the fit + classification.
    # Note we can omit the model_path argument, and in that case the
    # default model `data/models/default-model.obj` will be used.
    rfscore_args = ['cjd', 'cfid', 'cmagpsf', 'csigmapsf']
    df = df.withColumn(
        'rfscore',
        rfscore_sigmoid_full(*rfscore_args)
    )

    # Apply level one processor: superNNova
    logger.info("New processor: supernnova")

    snn_args = ['candid', 'cjd', 'cfid', 'cmagpsf', 'csigmapsf', F.lit('snn_snia_vs_nonia')]
    df = df.withColumn('snn_snia_vs_nonia', snn_ia(*snn_args))

    snn_args = ['candid', 'cjd', 'cfid', 'cmagpsf', 'csigmapsf', F.lit('snn_sn_vs_all')]
    df = df.withColumn('snn_sn_vs_all', snn_ia(*snn_args))

    # Apply level one processor: microlensing
    logger.info("New processor: microlensing")

    # broadcast models
    curdir = os.path.dirname(os.path.abspath(fspath))
    model_path = curdir + '/data/models/'
    rf, pca = load_external_model(model_path)
    spark = SparkSession.builder.getOrCreate()
    rfbcast = spark.sparkContext.broadcast(rf)
    pcabcast = spark.sparkContext.broadcast(pca)

    def mulens_wrapper(fid, magpsf, sigmapsf, magnr, sigmagnr, magzpsci, isdiffpos):
        """ Wrapper to pass broadcasted values from this scope to mulens

        see `fink_science.mulens.processor`
        """
        return mulens(
            fid, magpsf, sigmapsf, magnr,
            sigmagnr, magzpsci, isdiffpos,
            rfbcast.value, pcabcast.value)

    # Retrieve schema
    schema = load_mulens_schema_twobands()

    # Create standard UDF
    mulens_udf = F.udf(mulens_wrapper, schema)

    # Required alert columns - already computed for SN
    mulens_args = [
        'cfid', 'cmagpsf', 'csigmapsf',
        'cmagnr', 'csigmagnr', 'cmagzpsci', 'cisdiffpos']
    df = df.withColumn('mulens', mulens_udf(*mulens_args))

    # Apply level one processor: asteroids
    logger.info("New processor: asteroids")
    args_roid = [
        'cjd', 'cmagpsf',
        'candidate.ndethist', 'candidate.sgscore1',
        'candidate.ssdistnr', 'candidate.distpsnr1']
    df = df.withColumn('roid', roid_catcher(*args_roid))

    # Apply level one processor: nalerthist
    logger.info("New processor: nalerthist")
    df = df.withColumn('nalerthist', nalerthist(df['cmagpsf']))

    # Drop temp columns
    df = df.drop(*expanded)

    return df

@pandas_udf(StringType(), PandasUDFType.SCALAR)
def extract_fink_classification(
        cdsxmatch, roid, mulens_class_1, mulens_class_2,
        snn_snia_vs_nonia, snn_sn_vs_all, rfscore,
        ndethist, drb, classtar):
    """ Extract the classification of an alert based on module outputs

    See https://arxiv.org/abs/2009.10185 for more information
    """
    classification = pd.Series(['Unknown'] * len(cdsxmatch))
    ambiguity = pd.Series([0] * len(cdsxmatch))

    # Microlensing classification
    f_mulens = (mulens_class_1 == 'ML') & (mulens_class_2 == 'ML')

    # SN Ia
    snn1 = snn_snia_vs_nonia.astype(float) > 0.5
    snn2 = snn_sn_vs_all.astype(float) > 0.5
    active_learn = rfscore.astype(float) > 0.5
    low_ndethist = ndethist.astype(int) < 400
    high_drb = drb.astype(float) > 0.5
    high_classtar = classtar.astype(float) > 0.4
    early_ndethist = ndethist.astype(int) <= 20

    list_simbad_galaxies = [
        "galaxy",
        "Galaxy",
        "EmG",
        "Seyfert",
        "Seyfert_1",
        "Seyfert_2",
        "BlueCompG",
        "StarburstG",
        "LSB_G",
        "HII_G",
        "High_z_G",
        "GinPair",
        "GinGroup",
        "BClG",
        "GinCl",
        "PartofG",
    ]
    keep_cds = \
        ["Unknown", "Candidate_SN*", "SN", "Transient"] + list_simbad_galaxies

    f_sn = (snn1 | snn2) & cdsxmatch.isin(keep_cds) & low_ndethist & high_drb & high_classtar
    f_sn_early = early_ndethist & active_learn & f_sn

    # Solar System Objects
    f_roid = roid.astype(int).isin([2, 3])

    # Simbad xmatch
    f_simbad = ~cdsxmatch.isin(['Unknown', 'Transient', 'Fail'])

    classification.mask(f_mulens.values, 'Microlensing candidate', inplace=True)
    classification.mask(f_sn.values, 'SN candidate', inplace=True)
    classification.mask(f_sn_early.values, 'Early SN candidate', inplace=True)
    classification.mask(f_roid.values, 'Solar System', inplace=True)

    # If several flags are up, we cannot rely on the classification
    ambiguity[f_mulens.values] += 1
    ambiguity[f_sn.values] += 1
    ambiguity[f_roid.values] += 1
    f_ambiguity = ambiguity > 1
    classification.mask(f_ambiguity.values, 'Ambiguous', inplace=True)

    classification = np.where(f_simbad, cdsxmatch, classification)

    return pd.Series(classification)


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    globs = globals()
    root = os.environ['FINK_HOME']
    globs["ztf_alert_sample"] = os.path.join(
        root, "ztf_alerts/raw")

    # Run the Spark test suite
    spark_unit_tests(globs)
