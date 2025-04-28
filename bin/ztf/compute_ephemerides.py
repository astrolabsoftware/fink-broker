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
from fink_utils.sso.ephem import extract_ztf_ephemerides_from_miriade
from fink_utils.sso.ephem import expand_columns

SSO_FILE = "sso_ztf_lc_aggregated_{}{}.parquet"


def aggregate_and_add_ephem(year, npart, prefix_path, limit, logger):
    """Wrapper to get new ZTF data and ephemerides

    Parameters
    ----------
    year: str
        Year in format YYYY

    Returns
    -------
    out: Spark DataFrame
    """
    logger.info("Aggregating data from {}".format(year))
    current_year = datetime.datetime.today().year
    df_new = aggregate_ztf_sso_data(
        year=year, stop_previous_month=(year == current_year), prefix_path=prefix_path
    )

    if limit is not None:
        assert isinstance(limit, int), (limit, type(limit))
        logger.info("Limiting the new number of objects to {}".format(limit))
        df_new = df_new.limit(limit)

    df_new = df_new.repartition(npart).cache()
    logger.info("{} objects".format(df_new.count()))

    logger.info("Aggregating ephemerides from {}".format(year))
    col_ = "ephem"
    df_new = df_new.withColumn(
        col_,
        extract_ztf_ephemerides_from_miriade(
            "ssnamenr", "cjd", F.expr("uuid()"), F.lit("ephemcc")
        ),
    )
    df_expanded = expand_columns(df_new, col_to_expand=col_)
    return df_expanded


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
    spark = init_sparksession(name="{}_ephemerides".format(args.mode), shuffle_partitions=100)
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

            if path_exist(SSO_FILE.format(year, "")):
                logger.warning(
                    "{} found on HDFS. Skipping the computation".format(
                        SSO_FILE.format(year, "")
                    )
                )
                is_starting = False
                continue

            is_data = path_exist("{}/year={}".format(args.prefix_path, year))
            if not is_data:
                # Check if there is data for the month
                logger.warn(
                    "No data found for {}/year={}. Skipping...".format(
                        args.prefix_path, year
                    )
                )
                continue

            if is_starting:
                logger.info("Initialising data from {}".format(year))
                # initialisation
                df = aggregate_and_add_ephem(
                    year, nparts, args.prefix_path, args.limit, logger
                )

                df.write.mode("overwrite").parquet(SSO_FILE.format(year, ""))
                is_starting = False
                continue

            df_new = aggregate_and_add_ephem(
                year, nparts, args.prefix_path, args.limit, logger
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


if __name__ == "__main__":
    main()
