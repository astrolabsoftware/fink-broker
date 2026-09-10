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

import os
import argparse
import datetime

from fink_broker.common.logging_utils import get_fink_logger, inspect_application
from fink_broker.common.spark_utils import init_sparksession

from fink_science.rubin.ssoft.processor import build_the_ssoft
from fink_utils.sso.utils import retrieve_last_date_of_previous_month
from fink_utils.hdfs.utils import path_exist


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
