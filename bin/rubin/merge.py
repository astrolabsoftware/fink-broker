#!/usr/bin/env python
# Copyright 2020-2026 AstroLab Software
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
"""Retrieve one LSST night, and merge small files into larger ones."""

from pyspark.sql import functions as F

import argparse

from fink_broker.common.parser import getargs
from fink_broker.common.spark_utils import init_sparksession
from fink_broker.common.partitioning import convert_to_datetime, compute_num_part
from fink_broker.common.logging_utils import get_fink_logger, inspect_application
# from fink_broker.ztf.tracklet_identification import add_tracklet_information


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(name="lsst_mergeAndClean_{}".format(args.night))

    # Logger to print useful debug statements
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    print("Processing {}".format(args.night))

    input_science = "{}/science/{}".format(args.online_data_prefix, args.night)

    # basepath
    output_science = "{}/science".format(args.agg_data_prefix)

    print("Science data processing....")
    df_science = spark.read.format("parquet").load(input_science)
    npart_after = int(compute_num_part(df_science))
    print("Num partitions before: ", df_science.rdd.getNumPartitions())
    print("Num partitions after : ", npart_after)

    # Add tracklet information before merging
    # df_trck = add_tracklet_information(df_science)

    # join back information to the initial dataframe
    # df_science = df_science.join(
    #     F.broadcast(df_trck.select(["candid", "tracklet"])), on="candid", how="outer"
    # )

    if "local" in spark.conf.get("spark.master"):
        # only for tests
        df_science.withColumn(
            "timestamp",
            convert_to_datetime(df_science["diaSource.midpointMjdTai"], F.lit("mjd")),
        ).withColumn("year", F.lit(args.night[0:4])).withColumn(
            "month", F.lit(args.night[4:6])
        ).withColumn("day", F.lit(args.night[6:8])).coalesce(npart_after).write.mode(
            "append"
        ).partitionBy("year", "month", "day").parquet(output_science)
    else:
        df_science.withColumn(
            "timestamp",
            convert_to_datetime(df_science["diaSource.midpointMjdTai"], F.lit("mjd")),
        ).withColumn("year", F.year("timestamp")).withColumn(
            "month", F.lpad(F.month("timestamp").cast("string"), 2, "0")
        ).withColumn(
            "day", F.lpad(F.dayofmonth("timestamp").cast("string"), 2, "0")
        ).coalesce(npart_after).write.mode("append").partitionBy(
            "year", "month", "day"
        ).parquet(output_science)


if __name__ == "__main__":
    main()
