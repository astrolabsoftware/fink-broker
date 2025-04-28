# Copyright 2019-2025 AstroLab Software
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
import datetime
import argparse

import pyspark.sql.functions as F

from fink_broker.common.logging_utils import get_fink_logger, inspect_application
from fink_broker.common.spark_utils import init_sparksession

from fink_utils.hdfs.utils import path_exist
from fink_utils.sso.ssoft import aggregate_ztf_sso_data
from fink_utils.sso.ssoft import join_aggregated_sso_data
from fink_utils.sso.ssoft import retrieve_last_date_of_previous_month
from fink_utils.sso.ephem import extract_ztf_ephemerides_from_miriade
from fink_utils.sso.ephem import expand_columns

SSO_FILE = "sso_ztf_lc_aggregated_{}{}.parquet"


def aggregate_and_add_ephem(year, month, npart, prefix_path, limit, logger):
    """Wrapper to get new ZTF data and ephemerides

    Parameters
    ----------
    year: str
        Year in format YYYY
    month: str
        Month in format MM. Leading zero must be there.
        For yearly aggregation, set month to None.
    npart: int
        Number of Spark partitions. Rule of thumb: 4 times
        the number of cores.
    prefix_path: str
        Prefix path to Fink/ZTF data
    limit: int
        If set, limit the number of object to process.
        Otherwise, put to None.


    Returns
    -------
    out: Spark DataFrame
    """
    if month is None:
        logger.info("Aggregating data from {}".format(year))
        current_year = datetime.datetime.now().year
        df_new = aggregate_ztf_sso_data(
            year=year,
            stop_previous_month=(year == current_year),
            prefix_path=prefix_path,
        )
    else:
        logger.info("Aggregating data from {}{}".format(year, month))
        df_new = aggregate_ztf_sso_data(year=year, month=month, prefix_path=prefix_path)

    if limit is not None:
        assert isinstance(limit, int), (limit, type(limit))
        logger.info("Limiting the new number of objects to {}".format(limit))
        df_new = df_new.limit(limit)

    df_new = df_new.repartition(npart).cache()
    logger.info("{} objects".format(df_new.count()))

    if month is None:
        logger.info("Aggregating ephemerides from {}".format(year))
    else:
        logger.info("Aggregating ephemerides from {}{}".format(year, month))
    col_ = "ephem"
    df_new = df_new.withColumn(
        col_,
        extract_ztf_ephemerides_from_miriade(
            "ssnamenr", "cjd", F.expr("uuid()"), F.lit("ephemcc")
        ),
    )
    df_expanded = expand_columns(df_new, col_to_expand=col_)
    return df_expanded


def make_checks(prefix_path, year=None, monthly=None, logger=None):
    """Check if the ephemerides file and Fink data exist

    Notes
    -----
    If the ephemerides file exist, the recomputation will be skipped.
    If no Fink data is found, the aggregation will be skipped.

    Parameters
    ----------
    prefix_path: str
        Prefix path to Fink/ZTF data
    year: int, optional
        Year in format YYYY. If None, assume
        `monthly` is set. Default is None.
    monthly: bool
        If True, check ephemerides and data required for
        the current month computation. Default is None.

    Notes
    -----
    `year` is only when recomputing all ephemerides, while
    `monthly` is used in prod when computing last month ephemerides.

    Returns
    -------
    is_ephem: bool
        True if the ephemerides file exist.
    is_data: bool
        False if no Fink data for YYYY[MM].
    """
    if logger is None:
        import logging

        logger = logging.Logger(__name__)

    if monthly:
        curr = datetime.datetime.now()

        # ephemerides take current month
        filename = SSO_FILE.format(curr.year, "{:02d}".format(curr.month))
        is_ephem = path_exist(filename)

        # ZTF data takes N-1 month
        lm = retrieve_last_date_of_previous_month(curr)
        path = "{}/year={}/month={}".format(
            prefix_path, lm.year, "{:02d}".format(lm.month)
        )
        is_data = path_exist(path)
    elif year is not None:
        filename = SSO_FILE.format(year, "")
        is_ephem = path_exist(filename)

        # ZTF data takes current year
        path = "{}/year={}".format(prefix_path, year)
        is_data = path_exist(path)

    if is_ephem:
        logger.warning("{} found on HDFS. Skipping the computation".format(filename))

    if not is_data:
        logger.warning("No data found for {}. Skipping...".format(path))
    return is_ephem, is_data


def main():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        "-limit",
        type=int,
        default=None,
        help="""
        Use only `limit` number of SSO per month for test purposes.
        Default is None, meaning all available data is considered.
        """,
    )
    parser.add_argument(
        "-prefix_path",
        type=str,
        default="archive/science",
        help="""
        Prefix path on HDFS to the yearly data.
        Default is "archive/science".
        """,
    )
    parser.add_argument(
        "-mode",
        type=str,
        default="last_month",
        help="""
        Compute last month ephemerides `last_month`, or recompute
        all ephemerides `all`.
        """,
    )
    args = parser.parse_args(None)

    # Initialise Spark session
    spark = init_sparksession(
        name="{}_ephemerides".format(args.mode), shuffle_partitions=100
    )
    ncores = int(spark.sparkContext.getConf().get("spark.cores.max"))

    # 4 times more partitions than cores
    nparts = 4 * ncores

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, "INFO")

    # debug statements
    inspect_application(logger)

    if args.mode == "all":
        years = range(2019, datetime.datetime.now().year + 1)
        is_starting = True
        for year in years:
            logger.info("Processing data from {}".format(year))

            # Skip computation if necessary
            is_ephem, is_data = make_checks(
                args.prefix_path, yearly=True, logger=logger
            )
            if is_ephem:
                is_starting = False
                continue
            if not is_data:
                continue

            if is_starting:
                logger.info("Initialising data from {}".format(year))
                # initialisation
                df = aggregate_and_add_ephem(
                    year, None, nparts, args.prefix_path, args.limit, logger
                )

                df.write.mode("overwrite").parquet(SSO_FILE.format(year, ""))
                is_starting = False
                continue

            df_new = aggregate_and_add_ephem(
                year, None, nparts, args.prefix_path, args.limit, logger
            )

            logger.info("Loading previous data...")
            df_prev = spark.read.format("parquet").load(SSO_FILE.format(year - 1, ""))

            logger.info("Joining previous and new data...")
            assert sorted(df_prev.columns) == sorted(df_new.columns), (
                df_prev.columns,
                df_new.columns,
            )
            df_join = join_aggregated_sso_data(df_prev, df_new, on="ssnamenr")

            df_join.write.mode("overwrite").parquet(SSO_FILE.format(year, ""))
    elif args.mode == "last_month":
        # get last month coordinates
        lm = retrieve_last_date_of_previous_month(datetime.datetime.now())

        # make checks
        is_ephem, is_data = make_checks(args.prefix_path, monthly=True, logger=logger)
        if not is_ephem and is_data:
            # make computation
            df_new = aggregate_and_add_ephem(
                lm.year,
                "{:02d}".format(lm.month),
                nparts,
                args.prefix_path,
                args.limit,
                logger,
            )

            logger.info("Loading previous ephemerides data...")
            df_prev = spark.read.format("parquet").load(
                SSO_FILE.format(lm.year, "{:02d}".format(lm.month))
            )

            logger.info("Joining previous and new data...")
            assert sorted(df_prev.columns) == sorted(df_new.columns), (
                df_prev.columns,
                df_new.columns,
            )
            df_join = join_aggregated_sso_data(df_prev, df_new, on="ssnamenr")

            current_month = "{:02d}".format(datetime.datetime.now().month)
            df_join.write.mode("overwrite").parquet(
                SSO_FILE.format(year, current_month)
            )
        else:
            return 1


if __name__ == "__main__":
    main()
