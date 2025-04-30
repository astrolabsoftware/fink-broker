#!/usr/bin/env python
# Copyright 2023-2025 AstroLab Software
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
"""Construct the Solar System Object Fink Table (SSOFT)."""

import argparse
import datetime

from fink_broker.common.logging_utils import get_fink_logger, inspect_application
from fink_broker.common.spark_utils import init_sparksession

from fink_science.ztf.ssoft.processor import build_the_ssoft
from fink_utils.hdfs.utils import path_exist


# Defined in `fink_ssoft -s ztf --link-data`
SSO_FILE = "sso_ztf_lc_aggregated.parquet"


def main():
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
        default=50,
        help="""
        Minimum number of points in the lightcurve of an
        object to be considered for the SSOFT. Default is 50
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

    if not path_exist(SSO_FILE):
        logger.warn("{} does not exist".format(SSO_FILE))

    # TODO: define limit instead of frac in build_the_ssoft
    if args.limit is not None:
        frac = args.limit / 1e5

    pdf = build_the_ssoft(
        aggregated_filename=SSO_FILE,
        nparts=nparts,
        nmin=args.nmin,
        frac=frac,
        model=args.model,
        version=version,
        sb_method="fastnifty",
    )

    pdf.to_parquet("ssoft_{}_{}.parquet".format(args.model, version))


if __name__ == "__main__":
    main()
