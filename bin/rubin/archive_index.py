#!/usr/bin/env python
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

"""Push science data to the science portal (HBase table)

1. Use the Alert data that is stored in the Science TMP database (Parquet)
2. Extract relevant information from alerts
3. Construct HBase catalog
4. Push data (single shot)
"""

import sys
import argparse

import pyspark.sql.functions as F
from pyspark.sql import Window

from fink_broker.common.spark_utils import init_sparksession, load_parquet_files
from fink_broker.common.spark_utils import ang2pix
from fink_broker.common.spark_utils import list_hdfs_files

from fink_broker.common.logging_utils import get_fink_logger, inspect_application
from fink_broker.common.parser import getargs

from fink_broker.rubin.spark_utils import get_schema_from_parquet

from fink_broker.rubin.hbase_utils import ingest_section


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # construct the index view
    index_row_key_name = args.index_table
    columns = index_row_key_name.split("_")
    index_name = "." + columns[0]

    # Initialise Spark session
    spark = init_sparksession(
        name="index_archival_{}_{}".format(args.index_table, args.night),
        shuffle_partitions=2,
    )

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    # Connect to the aggregated science database
    path = "{}/science/year={}/month={}/day={}".format(
        args.agg_data_prefix, args.night[:4], args.night[4:6], args.night[6:8]
    )
    paths = list_hdfs_files(path)

    # Load all columns
    major_version, minor_version = get_schema_from_parquet(paths)

    nfiles = 100
    nloops = int(len(paths) / nfiles) + 1

    logger.info("{} parquet detected ({} loops to perform)".format(len(paths), nloops))

    # incremental push
    for index in range(0, len(paths), nfiles):
        logger.info("Loop {}/{}".format(index + 1, nloops))
        df = load_parquet_files(paths[index : index + nfiles])

        # Keep only alerts diaObject
        # This will discard SSO
        df = df.filter(~df["diaObject"].isNull())

        if columns[0].startswith("pixel"):
            nside = int(columns[0].split("pixel")[1])

            df = df.withColumn(
                columns[0],
                ang2pix(df["diaSource.ra"], df["diaSource.dec"], F.lit(nside)),
            )

            # Keep only the last alert per object
            w = Window.partitionBy(
                "{}.{}".format("diaObject", "diaObjectId")
            ).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            df_dedup = (
                df
                .withColumn("maxMjd", F.max("diaSource.midpointMjdTai").over(w))
                .where(F.col("diaSource.midpointMjdTai") == F.col("maxMjd"))
                .drop("maxMjd")
            )

            ingest_section(
                df_dedup,
                major_version,
                minor_version,
                index_row_key_name,
                args.science_db_name + index_name,
                args.science_db_catalogs,
            )
        # elif columns[0] == "forcedSources":
        #     # FIXME: for lsst schema v8, rewrite with conditions on `diaObject.nDiaSources`
        #     # FIXME: see https://github.com/astrolabsoftware/fink-broker/issues/1016

        #     # step 1
        #     # The logic would be that we filter on F.size("prvDiaSources") > 0
        #     # but "prvDiaSources" is null if not provided
        #     df_nonnull = df.filter(F.col("prvDiaSources").isNotNull())

        #     # step 2 -- set maxMidpointMjdTai
        #     df_withlast = df_nonnull.withColumn(
        #         "maxMidpointMjdTai",
        #         F.when(
        #             F.size("prvDiaSources") == 2,
        #             0.0,  # Return ridiculously small number to not filter out afterwards
        #         ).otherwise(
        #             F.expr(
        #                 "aggregate(prvDiaSources, cast(-1.0 as double), (acc, x) -> greatest(acc, x.midpointMjdTai))"
        #             )
        #         ),
        #     )

        #     # step 3 -- filter
        #     df_filtered = df_withlast.filter(
        #         ~F.col("maxMidpointMjdTai").isNull()
        #     ).withColumn(
        #         "prvDiaForcedSources_filt",
        #         F.expr(
        #             "filter(prvDiaForcedSources, x -> x.midpointMjdTai >= maxMidpointMjdTai)"
        #         ),
        #     )

        #     # Flatten the DataFrame for HBase ingestion
        #     df_flat = (
        #         df_filtered.select("prvDiaForcedSources_filt")
        #         .select(F.explode("prvDiaForcedSources_filt").alias("forcedSource"))
        #         .select("forcedSource.*")
        #     )

        #     # Columns to store
        #     cf = assign_column_family_names(
        #         df_flat,
        #         cols_i=df_flat.columns,
        #         cols_d=[],
        #     )

        #     # add salt
        #     df_flat = salt_from_last_digits(
        #         df_flat, colname="diaObjectId", npartitions=npartitions
        #     )

        #     # Add row key
        #     index_row_key_name = "salt_diaObjectId_midpointMjdTai"
        #     df_flat = add_row_key(
        #         df_flat,
        #         row_key_name=index_row_key_name,
        #         cols=index_row_key_name.split("_"),
        #     )

        #     # Salt not needed anymore
        #     df_flat = df_flat.drop("salt")

        else:
            logger.warning("{} is not a supported index name.".format(columns[0]))
            sys.exit(1)

        # push_to_hbase(
        #     df=df_flat,
        #     table_name=args.science_db_name + index_name,
        #     rowkeyname=index_row_key_name,
        #     cf=cf,
        #     catfolder=args.science_db_catalogs,
        # )


if __name__ == "__main__":
    main()
