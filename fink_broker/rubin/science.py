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
from fink_broker.common.science import apply_all_xmatch

from fink_science.rubin.random_forest_snia.processor import (
    rfscore_rainbow_elasticc_nometa,
)
from fink_science.rubin.snn.processor import snn_ia_elasticc
from fink_science.rubin.cats.processor import predict_nn
from fink_science.rubin.slsn.processor import slsn_rubin

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_LOG = logging.getLogger(__name__)


def apply_science_modules(df: DataFrame, tns_raw_output: str = "") -> DataFrame:
    """Load and apply Fink science modules to enrich Rubin alert content

    Currently available:
    - CBPF (broad)
    - SNN (Ia)
    - SLSN
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
    >>> from fink_broker.common.logging_utils import get_fink_logger
    >>> logger = get_fink_logger('raw2cience_rubin_test', 'INFO')
    >>> _LOG = logging.getLogger(__name__)
    >>> df = load_parquet_files(rubin_alert_sample)
    >>> df = apply_science_modules(df)
    >>> df.write.format("noop").mode("overwrite").save()
    """
    # Required alert columns
    to_expand = ["midpointMjdTai", "band", "psfFlux", "psfFluxErr"]

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

    df = apply_all_xmatch(df, tns_raw_output, survey="rubin")

    _LOG.info("New processor: asteroids (random positions)")
    df = df.withColumn("roid", F.lit(0))

    _LOG.info("New processor: EarlySN Ia")
    early_ia_args = [F.col(i) for i in expanded]
    df = df.withColumn(
        "rf_snia_vs_nonia", rfscore_rainbow_elasticc_nometa(*early_ia_args)
    )

    _LOG.info("New processor: supernnova - Ia")
    snn_args = [F.col("diaSource.diaSourceId")]
    snn_args += [F.col(i) for i in expanded]

    # # Binary Ia
    # snn_ia_args = snn_args + [F.lit("elasticc_ia")]
    # df = df.withColumn("snn_snia_vs_nonia", snn_ia_elasticc(*snn_ia_args))

    # Binary SN
    snn_ia_args = snn_args + [F.lit("elasticc_binary_broad/SN_vs_other")]
    df = df.withColumn("snn_sn_vs_others", snn_ia_elasticc(*snn_ia_args))

    # # Binary Periodic
    # full_args = args + [F.lit("elasticc_binary_broad/Periodic_vs_other")]
    # df = df.withColumn("snn_periodic_vs_others", snn_ia_elasticc(*full_args))

    # # Binary nonperiodic
    # full_args = args + [F.lit("elasticc_binary_broad/NonPeriodic_vs_other")]
    # df = df.withColumn("snn_nonperiodic_vs_others", snn_ia_elasticc(*full_args))

    # # Binary Long
    # full_args = args + [F.lit("elasticc_binary_broad/Long_vs_other")]
    # df = df.withColumn("snn_long_vs_others", snn_ia_elasticc(*full_args))

    # # Binary Fast
    # full_args = args + [F.lit("elasticc_binary_broad/Fast_vs_other")]
    # df = df.withColumn("snn_fast_vs_others", snn_ia_elasticc(*full_args))

    # _LOG.info("New processor: supernnova - Broad")
    # args = [F.col("diaSource.diaSourceId")]
    # args += [
    #     F.col("cmidPointTai"),
    #     F.col("cfilterName"),
    #     F.col("cpsFlux"),
    #     F.col("cpsFluxErr"),
    # ]
    # args += [F.col("roid"), F.col("cdsxmatch"), F.array_min("cmidPointTai")]
    # args += [F.col("diaObject.mwebv"), F.col("redshift"), F.col("redshift_err")]
    # args += [F.lit("elasticc_broad")]
    # df = df.withColumn("preds_snn", snn_broad_elasticc(*args))

    # mapping_snn = {
    #     0: 11,
    #     1: 13,
    #     2: 12,
    #     3: 22,
    #     4: 21,
    # }
    # mapping_snn_expr = F.create_map([F.lit(x) for x in chain(*mapping_snn.items())])

    # df = df.withColumn(
    #     "snn_argmax", F.expr("array_position(preds_snn, array_max(preds_snn)) - 1")
    # )
    # df = df.withColumn("snn_broad_class", mapping_snn_expr[df["snn_argmax"]])
    # df = df.withColumnRenamed("preds_snn", "snn_broad_array_prob")

    # CATS
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
    df = df.withColumn("cats_broad_class", mapping_cats_general_expr[df["cats_argmax"]])

    # SLSN
    slsn_args = ["diaObject.diaObjectId"]
    slsn_args += [F.col(i) for i in expanded]
    slsn_args += ["diaSource.ra", "diaSource.dec"]
    df = df.withColumn("rf_slsn_vs_nonslsn", slsn_rubin(*slsn_args))

    # Drop temp columns
    df = df.drop(*expanded)
    df = df.drop(*[
        "cats_broad_array_prob",
        "cats_argmax",
    ])

    return df


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    globs = globals()
    root = os.environ["FINK_HOME"]
    globs["rubin_alert_sample"] = os.path.join(root, "datasim/or4_lsst7.1")

    # Run the Spark test suite
    spark_unit_tests(globs)
