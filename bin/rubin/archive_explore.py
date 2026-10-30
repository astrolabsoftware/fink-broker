#!/usr/bin/env python
# Copyright 2019-2026 AstroLab Software
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
import sys
import argparse

from fink_broker.common.spark_utils import init_sparksession
from fink_broker.common.hbase_utils import add_row_key, push_to_hbase
from fink_broker.common.parser import getargs
from fink_broker.common.logging_utils import init_logger
from fink_broker.common.spark_utils import load_parquet_files
from fink_broker.common.spark_utils import list_hdfs_files
from fink_broker.rubin.spark_utils import get_schema_from_parquet
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def reduce_df_size(df, colname, maxnum=100):
    """Reduce the DataFrame to `maxnum` rows per `colname` occurence"""
    window_spec = Window.partitionBy(colname).orderBy(F.monotonically_increasing_id())
    df_unique = (
        df.withColumn("id", F.col("diaObject.diaObjectId"))
        .orderBy("diaSource.midpointMjdTai", ascending=False)
        .drop_duplicates(subset=["id"])
        .drop("id")
    )
    df_with_rank = df_unique.withColumn("row_num", F.row_number().over(window_spec))

    # Filter to keep only the first `maxnum` occurrences per class
    df_limited = df_with_rank.filter(F.col("row_num") <= maxnum)

    return df_limited


def push_to_hbase_catalog(df, col_id, index_name, science_db_name, science_db_catalogs):
    """Wrapper around push_to_hbase"""
    cols_row_key_name = ["class", "row_num"]
    row_key_name = "_".join(cols_row_key_name)

    df = add_row_key(df, row_key_name=row_key_name, cols=cols_row_key_name)
    df = df.select(
        [
            row_key_name,
            col_id,
            "diaSource.midpointMjdTai",
        ]
    )

    cf = {i: "r" for i in df.columns}

    push_to_hbase(
        df=df,
        table_name=science_db_name + index_name,
        rowkeyname=row_key_name,
        cf=cf,
        catfolder=science_db_catalogs,
    )


def main():
    """Table with a sample of interesting candidates. Supported: SIMBAD, TNS, VSX, MPC"""
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    logger = init_logger(args.log_level)

    # Table prefix
    index_name = ".explore"

    # Maximum number of samples
    maxnum = 100

    # Initialise Spark session
    init_sparksession(name="sso_resolver_{}".format(args.night), shuffle_partitions=2)

    # Connect to the aggregated science database
    folder = "{}/science/year={}/month={}/day={}".format(
        args.agg_data_prefix, args.night[:4], args.night[4:6], args.night[6:8]
    )

    # Get list of files
    paths = list_hdfs_files(folder)

    major_version, minor_version = get_schema_from_parquet(paths)

    if major_version < 10:
        logger.warning(
            "Version {} detected. Skipping SSO injection".format(major_version)
        )
        sys.exit()

    if len(paths) == 0:
        logger.warning("No parquet found at {}".format(folder))
        sys.exit()

    df = load_parquet_files(paths)

    logger.info("Pushing SIMBAD samples")
    df_filtered = (
        df.filter(~df["xm.simbad_otype"].isin("Fail"))
        .filter(df["xm.simbad_otype"].isNotNull())
        .filter(df["diaObject.diaObjectId"].isNotNull())
    )
    df_limited = reduce_df_size(df_filtered, colname="xm.simbad_otype", maxnum=maxnum)
    df_limited = df_limited.withColumn(
        "class",
        F.concat(
            F.lit("SIMBAD"),
            F.lit("/"),
            F.col("xm.simbad_otype"),
        ),
    )
    push_to_hbase_catalog(
        df_limited,
        "diaObject.diaObjectId",
        index_name,
        args.science_db_name,
        args.science_db_catalogs,
    )

    logger.info("Pushing TNS samples")
    df_filtered = df.filter(df["xm.tns_type"].isNotNull()).filter(
        df["diaObject.diaObjectId"].isNotNull()
    )
    df_limited = reduce_df_size(df_filtered, colname="xm.tns_type", maxnum=maxnum)
    df_limited = df_limited.withColumn(
        "class",
        F.concat(
            F.lit("TNS"),
            F.lit("/"),
            F.col("xm.tns_type"),
        ),
    )
    push_to_hbase_catalog(
        df_limited,
        "diaObject.diaObjectId",
        index_name,
        args.science_db_name,
        args.science_db_catalogs,
    )

    logger.info("Pushing VSX samples")
    df_filtered = df.filter(df["xm.vsx_Type"].isNotNull()).filter(
        df["diaObject.diaObjectId"].isNotNull()
    )
    df_limited = reduce_df_size(df_filtered, colname="xm.vsx_Type", maxnum=maxnum)
    df_limited = df_limited.withColumn(
        "class",
        F.concat(
            F.lit("VSX"),
            F.lit("/"),
            F.col("xm.vsx_Type"),
        ),
    )
    push_to_hbase_catalog(
        df_limited,
        "diaObject.diaObjectId",
        index_name,
        args.science_db_name,
        args.science_db_catalogs,
    )

    logger.info("Pushing SSO samples")
    df_filtered = df.filter(df["mpc_orbits"].isNotNull())
    df_unique = (
        df_filtered.withColumn(
            "id", F.col("mpc_orbits.packed_primary_provisional_designation")
        )
        .drop_duplicates(subset=["id"])
        .drop("id")
        .limit(maxnum)
    )
    df_limited = df_unique.withColumn("class", F.lit("SSO/MPC")).withColumn(
        "row_num", F.monotonically_increasing_id()
    )
    push_to_hbase_catalog(
        df_limited,
        "mpc_orbits.packed_primary_provisional_designation",
        index_name,
        args.science_db_name,
        args.science_db_catalogs,
    )


if __name__ == "__main__":
    main()
