#!/usr/bin/env python
# Copyright 2023-2024 AstroLab Software
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
"""Construct the Solar System Object Fink Table (SSOFT)"""
import argparse
import datetime

from fink_broker.loggingUtils import get_fink_logger, inspect_application
from fink_broker.sparkUtils import init_sparksession

from fink_spins.ssoft import build_the_ssoft

def main():
    parser = argparse.ArgumentParser(description=__doc__)

    # Add specific arguments
    parser.add_argument(
        '-model', type=str, default='SHG1G2',
        help="""
        Lightcurve model: SHG1G2, HG1G2, HG
        """
    )
    parser.add_argument(
        '-version', type=str, default=None,
        help="""
        Version to use in the format YYYY.MM
        Default is None, meaning current Year.Month
        """
    )
    parser.add_argument(
        '-frac', type=float, default=None,
        help="""
        Use only fraction (between 0 and 1) of the input dataset to build the SSOFT
        Default is None, meaning all available data is considered.
        """
    )
    parser.add_argument(
        '-nmin', type=int, default=50,
        help="""
        Minimum number of points in the lightcurve of an
        object to be considered for the SSOFT. Default is 50
        """
    )
    parser.add_argument(
        '--pre_aggregate_data', action="store_true",
        help="""
        If specified, aggregate and save data on HDFS before computing the SSOFT (slower).
        Otherwise, read pre-aggregated data on HDFS to compute the SSOFT (faster).
        """
    )
    args = parser.parse_args(None)

    if args.version is None:
        now = datetime.datetime.now()
        version = '{}.{:02d}'.format(now.year, now.month)
    else:
        version = args.version

    # Initialise Spark session
    spark = init_sparksession(
        name="ssoft_{}_{}".format(args.model, version),
        shuffle_partitions=2
    )

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, "INFO")

    # debug statements
    inspect_application(logger)

    # We map processing 1:1 with the cores
    ncores = int(spark.sparkContext.getConf().get("spark.cores.max"))
    print("NCORES: {}".format(ncores))

    if args.pre_aggregate_data:
        filename = None
    else:
        filename = 'sso_aggregated_{}'.format(version)

    pdf = build_the_ssoft(
        aggregated_filename=filename,
        nproc=ncores, nmin=args.nmin,
        frac=args.frac, model=args.model,
        version=version
    )

    pdf.to_parquet('ssoft_{}_{}.parquet'.format(args.model, version))


if __name__ == "__main__":
    main()
