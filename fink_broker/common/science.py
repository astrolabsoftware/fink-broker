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
from pyspark.sql import functions as F

import os
import sys
import logging

from fink_broker.common.tester import spark_unit_tests

_LOG = logging.getLogger(__name__)


def apply_all_xmatch(df, tns_raw_output, survey=""):
    """Apply all xmatch to a DataFrame

    Parameters
    ----------
    df: Spark DataFrame
        Spark DataFrame containing alert data
    survey: str
        Survey name: ztf, rubin

    Returns
    -------
    out: Spark DataFrame

    Examples
    --------
    >>> from fink_broker.common.spark_utils import load_parquet_files
    >>> df = load_parquet_files(ztf_alert_sample)
    >>> df = apply_all_xmatch(df, survey="ztf")

    # apply_science_modules is lazy, so trigger the computation
    >>> an_alert = df.take(1)

    >>> from fink_broker.common.spark_utils import load_parquet_files
    >>> df = load_parquet_files(rubin_alert_sample)
    >>> df = apply_all_xmatch(df, tns_raw_output="", survey="rubin")

    # apply_science_modules is lazy, so trigger the computation
    >>> an_alert = df.take(1)
    """
    if survey == "ztf":
        from fink_science.ztf.xmatch.processor import xmatch_cds
        from fink_science.ztf.xmatch.processor import xmatch_tns
        from fink_science.ztf.xmatch.processor import crossmatch_other_catalog
        from fink_science.ztf.xmatch.processor import crossmatch_mangrove

        alert_id = "candidate.candid"
        ra = "candidate.ra"
        dec = "candidate.dec"
    elif survey == "rubin":
        from fink_science.rubin.xmatch.processor import xmatch_cds
        from fink_science.rubin.xmatch.processor import xmatch_tns
        from fink_science.rubin.xmatch.processor import crossmatch_other_catalog
        from fink_science.rubin.xmatch.processor import crossmatch_mangrove

        alert_id = "diaSource.diaSourceId"
        ra = "diaSource.ra"
        dec = "diaSource.dec"
    else:
        _LOG.fatal("{} survey not supported. Choose among: ztf, rubin".format(survey))
        sys.exit(1)

    _LOG.info("New processor: cdsxmatch")
    df = xmatch_cds(df)

    _LOG.info("New processor: TNS")
    df = xmatch_tns(df, tns_raw_output=tns_raw_output)

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
            df[alert_id],
            df[ra],
            df[dec],
            F.lit("gcvs"),
        ),
    )

    _LOG.info("New processor: 3HSP (1 arcmin)")
    df = df.withColumn(
        "x3hsp",
        crossmatch_other_catalog(
            df[alert_id],
            df[ra],
            df[dec],
            F.lit("3hsp"),
            F.lit(60.0),
        ),
    )

    _LOG.info("New processor: 4LAC (1 arcmin)")
    df = df.withColumn(
        "x4lac",
        crossmatch_other_catalog(
            df[alert_id],
            df[ra],
            df[dec],
            F.lit("4lac"),
            F.lit(60.0),
        ),
    )

    _LOG.info("New processor: Mangrove (1 acrmin)")
    df = df.withColumn(
        "mangrove",
        crossmatch_mangrove(df[alert_id], df[ra], df[dec], F.lit(60.0)),
    )

    return df


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    globs = globals()
    root = os.environ["FINK_HOME"]
    globs["ztf_alert_sample"] = os.path.join(root, "online/raw/20200101")
    globs["rubin_alert_sample"] = os.path.join(root, "datasim/or4_lsst7.1")

    # Run the Spark test suite
    spark_unit_tests(globs)
