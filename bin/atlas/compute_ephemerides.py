# Copyright 2025 AstroLab Software
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
import argparse

import pyspark.sql.functions as F

from fink_broker.common.logging_utils import get_fink_logger, inspect_application
from fink_broker.common.spark_utils import init_sparksession

from fink_utils.sso.ephem import extract_ztf_ephemerides_from_miriade
from fink_utils.sso.ephem import expand_columns

SSO_FILE = "sso_atlas_lc_aggregated_v3_{}.parquet"


def read_and_add_ephem(df, npart, limit, logger):
    """Wrapper to get new ZTF data and ephemerides

    Parameters
    ----------
    npart: int
        Number of Spark partitions. Rule of thumb: 4 times
        the number of cores.
    prefix_path: str
        Prefix path to ATLAS data
    limit: int
        If set, limit the number of object to process.
        Otherwise, put to None.


    Returns
    -------
    out: Spark DataFrame
    """
    if limit is not None:
        assert isinstance(limit, int), (limit, type(limit))
        logger.info("Limiting the new number of objects to {}".format(limit))
        df = df.limit(limit)

    df = df.repartition(npart).cache()
    logger.info("{} objects".format(df.count()))

    col_ = "ephem"
    df = df.withColumn(
        col_,
        extract_ztf_ephemerides_from_miriade(
            "name", "cjd_obs", "obscode", F.lit(0.0), F.expr("uuid()"), F.lit("ephemcc")
        ),
    )
    df_expanded = expand_columns(df, col_to_expand=col_)
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
        "-path",
        type=str,
        default="sso_aggregated_ATLAS_v3_only_ztf_objects_T05",
        help="""
        Path to ATLAS or ATLAS x ZTF data on HDFS.
        """,
    )
    parser.add_argument(
        "-observer",
        type=str,
        default="T05",
        help="""
        IAU code for ATLAS stations: 'M22', 'T05', 'T08', 'W68'
        """,
    )
    args = parser.parse_args(None)

    # Initialise Spark session
    spark = init_sparksession(
        name="atlas_ephemerides_{}".format(args.observer), shuffle_partitions=100
    )
    ncores = int(spark.sparkContext.getConf().get("spark.cores.max"))

    # 4 times more partitions than cores
    nparts = 4 * ncores

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, "INFO")

    # debug statements
    inspect_application(logger)

    logger.info("Aggregating ephemerides from {}".format(args.path))
    df = spark.read.format("parquet").load(args.path)

    # get station as str
    df = df.withColumn("obscode", F.element_at(df["ciauobs"], 1))

    # sanitize names for quaero
    df = df.withColumn("name", F.regexp_replace(df["name"], " ", "_"))

    # Compute ephemerides
    df = read_and_add_ephem(df, nparts, args.limit, logger)

    # Write data on HDFS
    df.write.mode("overwrite").parquet(SSO_FILE.format(args.observer))


if __name__ == "__main__":
    main()
