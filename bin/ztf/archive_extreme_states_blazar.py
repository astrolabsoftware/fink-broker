#!/usr/bin/env python
# Copyright 2025-2026 AstroLab Software
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
"""Run the blazar filters, and push data to HBase and Telegram."""

import pyspark.sql.functions as F

import logging
import argparse


from fink_broker.common.parser import getargs
from fink_broker.common.spark_utils import init_sparksession, load_parquet_files
from fink_broker.common.logging_utils import get_fink_logger, inspect_application
from fink_broker.ztf.hbase_utils import push_full_df_to_hbase


from fink_science.ztf.standardized_flux.processor import standardized_flux

from fink_utils.spark.utils import concat_col
from fink_utils.spark.utils import apply_user_defined_filter

_LOG = logging.getLogger(__name__)


def get_std_flux(df):
    """Get standardized flux

    Parameters
    ----------
    df: Spark DataFrame
        Input alert DataFrame

    Returns
    -------
    out: Spark DataFrame
        Input DataFrame with a new nested column `container`.
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
    return df


def main():
    """Run the blazar filters and send results to HBase and telegram"""
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(name="blazar_{}".format(args.night), shuffle_partitions=2)

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    # Connect to the aggregated science database
    path = "{}/science/year={}/month={}/day={}".format(
        args.agg_data_prefix, args.night[:4], args.night[4:6], args.night[6:8]
    )
    df = load_parquet_files(path)

    # Drop partitioning columns
    df = df.drop("year").drop("month").drop("day")

    # HBase -- Filter for low states in general (incl. new ones)
    # Drop images
    df = df.drop("cutoutScience").drop("cutoutTemplate").drop("cutoutDifference")

    # Extract columns from the blazar module
    df = df.withColumn(
        "instantness_low", F.col("blazar_stats").getItem("instantness_low")
    )
    df = df.withColumn(
        "robustness_low", F.col("blazar_stats").getItem("robustness_low")
    )
    df = df.withColumn(
        "instantness_high", F.col("blazar_stats").getItem("instantness_high")
    )
    df = df.withColumn(
        "robustness_high", F.col("blazar_stats").getItem("robustness_high")
    )

    # Low states
    f_low = "fink_filters.ztf.filter_blazar_low_state.filter.blazar_low_state"
    df_low_state = apply_user_defined_filter(df, f_low).cache()
    n_low_state = df_low_state.count()
    _LOG.info("{} new entries for low state".format(n_low_state))

    if n_low_state > 0:
        # Row key
        row_key_name = "jd_objectId"

        # push data to HBase
        push_full_df_to_hbase(
            df_low_state,
            row_key_name=row_key_name,
            table_name=args.science_db_name + ".low_state_blazars",
            catalog_name=args.science_db_catalogs,
        )

    # High states
    f_high = "fink_filters.ztf.filter_blazar_high_state.filter.blazar_high_state"
    df_high_state = apply_user_defined_filter(df, f_high).cache()
    n_high_state = df_high_state.count()
    _LOG.info("{} new entries for high state".format(n_high_state))

    if n_high_state > 0:
        # Row key
        row_key_name = "jd_objectId"

        # push data to HBase
        push_full_df_to_hbase(
            df_high_state,
            row_key_name=row_key_name,
            table_name=args.science_db_name + ".high_state_blazars",
            catalog_name=args.science_db_catalogs,
        )


if __name__ == "__main__":
    main()
