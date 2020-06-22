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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from logging import Logger
import json
import os

from fink_science.utilities import concat_col

from fink_science.random_forest_snia.processor import rfscore_sigmoid_full
from fink_science.xmatch.processor import cdsxmatch
from fink_science.snn.processor import snn_ia
from fink_science.microlensing.processor import mulens
from fink_science.microlensing.classifier import load_mulens_schema_twobands
from fink_science.microlensing.classifier import load_external_model

from fink_science.asteroids.processor import roid_catcher


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

    snn_args = ['candid', 'cjd', 'cfid', 'cmagpsf', 'csigmapsf']
    df = df.withColumn('snnscore', snn_ia(*snn_args))

    # Apply level one processor: microlensing
    logger.info("New processor: microlensing")

    # broadcast models
    curdir = os.path.dirname(os.path.abspath(fspath))
    model_path = curdir + '/data/models/'
    rf, pca = load_external_model(model_path)
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
    args_roid = ['cfid', 'cmagpsf', 'candidate.ssdistnr']
    df = df.withColumn('roid', roid_catcher(*args_roid))

    # Drop temp columns
    df = df.drop(*expanded)

    return df


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    globs = globals()
    root = os.environ['FINK_HOME']
    globs["ztf_alert_sample"] = os.path.join(
        root, "ztf_alerts/raw")

    # Run the Spark test suite
    spark_unit_tests(globs)
