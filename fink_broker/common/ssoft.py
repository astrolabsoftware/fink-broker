# Copyright 2023-2026 AstroLab Software
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
"""Construct the Rubin Solar System Object Fink Table (SSOFT)."""

import pyspark.sql.functions as F

import os
import argparse
import datetime

from fink_broker.common.logging_utils import get_fink_logger, inspect_application
from fink_broker.common.spark_utils import init_sparksession

from fink_science.rubin.ssoft.processor import build_the_ssoft
from fink_utils.sso.utils import retrieve_last_date_of_previous_month
from fink_utils.sso.ssoft import join_aggregated_sso_data
from fink_utils.hdfs.utils import path_exist
from fink_utils.sso.ssoft import aggregate_rubin_sso_data, aggregate_ztf_sso_data
from fink_utils.sso.ephem import extract_ephemerides_from_miriade
from fink_utils.sso.ephem import expand_columns


def generate_ssoft(sso_file: str):
    """Construct the Solar System Object Fink Table (SSOFT)

    Parameters
    ----------
    sso_file: str
        File defined in fink_ssoft -s <ztf|rubin> --link-data
    """
    parser = argparse.ArgumentParser(description=__doc__)

    # Add specific arguments
    parser.add_argument(
        "-model",
        type=str,
        default="SHG1G2",
        help="""
        Phase curve model: SHG1G2, HG1G2, HG
        """,
    )
    parser.add_argument(
        "-version",
        type=str,
        default=None,
        help="""
        Version to use in the final filename.
        Default is None, meaning current Year.Month is used.
        """,
    )
    parser.add_argument(
        "-limit",
        type=int,
        default=None,
        help="""
        If set, limit the number of object to process.
        Otherwise, put to None.
        """,
    )
    parser.add_argument(
        "-nmin",
        type=int,
        default=10,
        help="""
        Minimum number of points in the lightcurve of an
        object to be considered for the SSOFT. Default is 10
        """,
    )
    parser.add_argument(
        "-outfolder",
        type=str,
        default="/data/fink/ssoft",
        help="""
        Output folder to store the SSOFT. It must be
        on a regular filesystem (not HDFS).
        """,
    )
    args = parser.parse_args(None)

    if args.version is None:
        now = datetime.datetime.now()
        version = "{}{:02d}".format(now.year, now.month)
    else:
        version = args.version

    # Initialise Spark session
    spark = init_sparksession(
        name="ssoft_{}_{}".format(args.model, version), shuffle_partitions=200
    )

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, "INFO")

    # debug statements
    inspect_application(logger)

    # We map processing 1:1 with the cores
    ncores = int(spark.sparkContext.getConf().get("spark.cores.max"))
    nparts = 4 * ncores

    if not path_exist(sso_file):
        logger.warn("{} does not exist".format(sso_file))

    # TODO: define limit instead of frac in build_the_ssoft
    if args.limit is not None:
        frac = args.limit / 1e5
    else:
        frac = None

    pdf = build_the_ssoft(
        aggregated_filename=sso_file,
        nparts=nparts,
        nmin=args.nmin,
        frac=frac,
        model=args.model,
        version=version,
        sb_method="fastnifty",
    )
    outpath = os.path.join(
        args.outfolder, "ssoft_{}_{}.parquet".format(args.model, version)
    )
    pdf.to_parquet(outpath)


def make_checks(prefix_path, sso_file, year=None, monthly=None, logger=None):
    """Check if the ephemerides file and Fink data exist

    Notes
    -----
    If the ephemerides file exist, the recomputation will be skipped.
    If no Fink data is found, the aggregation will be skipped.

    Parameters
    ----------
    prefix_path: str
        Prefix path to Fink/ZTF data
    sso_file: str
        ???
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
        filename = sso_file.format(curr.year, "{:02d}".format(curr.month))
        is_ephem = path_exist(filename)

        # ZTF data takes N-1 month
        lm = retrieve_last_date_of_previous_month(curr)
        path = "{}/year={}/month={}".format(
            prefix_path, lm.year, "{:02d}".format(lm.month)
        )
        is_data = path_exist(path)
    elif year is not None:
        filename = sso_file.format(year, "")
        is_ephem = path_exist(filename)

        # ZTF data takes current year
        path = "{}/year={}".format(prefix_path, year)
        is_data = path_exist(path)

    if is_ephem:
        logger.warning("{} found on HDFS. Skipping the computation".format(filename))

    if not is_data:
        logger.warning("No data found for {}. Skipping...".format(path))
    return is_ephem, is_data


def compute_ephemerides(survey: str, sso_file: str):
    """Compute ephemerides from alert data

    Parameters
    ----------
    survey: str
        ztf or rubin
    sso_file: str
        File defined in fink_ssoft -s <ztf|rubin> --link-data
    """
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
        logger.info("Processing data from {} to {}".format(years[0], years[-1]))

        if survey == "rubin":
            df_new = aggregate_and_add_ephem_rubin(
                None, None, nparts, args.prefix_path, args.limit, logger
            )
        elif survey == "ztf":
            df_new = aggregate_and_add_ephem_ztf(
                None, None, nparts, args.prefix_path, args.limit, logger
            )

        df_new.write.mode("overwrite").parquet(sso_file.format(years[-1], ""))
    elif args.mode == "last_month":
        # get last month coordinates
        lm = retrieve_last_date_of_previous_month(datetime.datetime.now())

        # make checks
        is_ephem, is_data = make_checks(
            args.prefix_path, sso_file, monthly=True, logger=logger
        )
        if not is_ephem and is_data:
            # make computation
            if survey == "rubin":
                df_new = aggregate_and_add_ephem_rubin(
                    lm.year,
                    "{:02d}".format(lm.month),
                    nparts,
                    args.prefix_path,
                    args.limit,
                    logger,
                )
            elif survey == "ztf":
                df_new = aggregate_and_add_ephem_ztf(
                    lm.year,
                    "{:02d}".format(lm.month),
                    nparts,
                    args.prefix_path,
                    args.limit,
                    logger,
                )

            logger.info("Loading previous ephemerides data...")
            prev_ephem_folder = sso_file.format(lm.year, "{:02d}".format(lm.month))
            if not path_exist(prev_ephem_folder):
                logger.warning(
                    "{} does not exist. Exiting...".format(prev_ephem_folder)
                )
                return 1

            df_prev = spark.read.format("parquet").load(
                sso_file.format(lm.year, "{:02d}".format(lm.month))
            )

            logger.info("Joining previous and new data...")
            assert sorted(df_prev.columns) == sorted(df_new.columns), (
                df_prev.columns,
                df_new.columns,
            )
            df_join = join_aggregated_sso_data(df_prev, df_new, on="ssnamenr")

            curr = datetime.datetime.now()
            current_month = "{:02d}".format(curr.month)
            df_join.write.mode("overwrite").parquet(
                sso_file.format(curr.year, current_month)
            )
        else:
            return 1


def aggregate_and_add_ephem_rubin(year, month, npart, prefix_path, limit, logger):
    """Wrapper to get new Rubin data and ephemerides

    Notes
    -----
    We might need to enable weekly aggregation.

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
        Prefix path to Fink/Rubin data
    limit: int
        If set, limit the number of object to process.
        Otherwise, put to None.


    Returns
    -------
    out: Spark DataFrame
    """
    if (year is None) and (month is None):
        logger.info("Aggregating ALL data")
        df_new = aggregate_rubin_sso_data(prefix_path=prefix_path)
    elif (year is not None) and (month is None):
        logger.info("Aggregating data from {}".format(year))
        current_year = datetime.datetime.now().year
        df_new = aggregate_rubin_sso_data(
            year=year,
            stop_previous_month=(year == current_year),
            prefix_path=prefix_path,
        )
    elif (year is not None) and (month is not None):
        logger.info("Aggregating data from {}{}".format(year, month))
        df_new = aggregate_rubin_sso_data(
            year=year, month=month, prefix_path=prefix_path
        )

    if limit is not None:
        assert isinstance(limit, int), (limit, type(limit))
        logger.info("Limiting the new number of objects to {}".format(limit))
        df_new = df_new.limit(limit)

    df_new = df_new.repartition(npart).cache()
    logger.info("{} objects".format(df_new.count()))

    col_ = "ephem"
    df_new = df_new.withColumn(
        col_,
        extract_ephemerides_from_miriade(
            "designation",
            "cjdUtc",
            F.lit("X05"),
            F.lit(0.0),
            F.expr("uuid()"),
            F.lit("ephemcc"),
            F.lit("ephemcc-photom.xml"),
        ),
    )
    df_expanded = expand_columns(df_new, col_to_expand=col_)
    return df_expanded


def aggregate_and_add_ephem_ztf(year, month, npart, prefix_path, limit, logger):
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
    if (year is None) and (month is None):
        logger.info("Aggregating ALL data")
        df_new = aggregate_ztf_sso_data(prefix_path=prefix_path)
    elif (year is not None) and (month is None):
        logger.info("Aggregating data from {}".format(year))
        current_year = datetime.datetime.now().year
        df_new = aggregate_ztf_sso_data(
            year=year,
            stop_previous_month=(year == current_year),
            prefix_path=prefix_path,
        )
    elif (year is not None) and (month is not None):
        logger.info("Aggregating data from {}{}".format(year, month))
        df_new = aggregate_ztf_sso_data(year=year, month=month, prefix_path=prefix_path)

    if limit is not None:
        assert isinstance(limit, int), (limit, type(limit))
        logger.info("Limiting the new number of objects to {}".format(limit))
        df_new = df_new.limit(limit)

    df_new = df_new.repartition(npart).cache()
    logger.info("{} objects".format(df_new.count()))

    col_ = "ephem"
    df_new = df_new.withColumn(
        col_,
        extract_ephemerides_from_miriade(
            "ssnamenr",
            "cjd",
            F.lit("I41"),
            F.lit(15.0),
            F.expr("uuid()"),
            F.lit("ephemcc"),
            F.lit("ephemcc-photom.xml"),
        ),
    )
    df_expanded = expand_columns(df_new, col_to_expand=col_)
    return df_expanded
