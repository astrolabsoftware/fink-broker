#!/usr/bin/env python
# Copyright 2025 AstroLab Software
# Author: Julien Peloton, Roman Le Montagner
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
"""Extract Avro files for test puroposes"""

import pkgutil
import argparse
import logging

from fink_broker.common.parser import getargs
from fink_broker.common.spark_utils import init_sparksession, load_parquet_files
from fink_broker.common.logging_utils import init_logger
from fink_utils.spark.utils import concat_col
from fink_utils.spark.utils import apply_user_defined_filter
import fink_filters.ztf.livestream as ffzl


_LOG = logging.getLogger(__name__)

# User-defined topics
userfilters = [
    "{}.{}.filter.{}".format(ffzl.__package__, mod, mod.split("filter_")[1])
    for _, mod, _ in pkgutil.iter_modules(ffzl.__path__)
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    logger = init_logger(args.log_level)

    logger.debug("Initialise Spark session")
    init_sparksession(
        name="generate_test_data_{}".format(args.producer),
        shuffle_partitions=10,
        log_level=args.spark_log_level,
    )

    # data path is fixed to first week of September 2025
    # basepath = "{}/{}/year=2025/month=09".format(args.agg_data_prefix, "{}")
    # paths = [basepath + "/day={:02d}".format(day) for day in range(1, 8)]
    paths = ["{}/{}/year=2023/month=10/day=18".format(args.agg_data_prefix, "{}")]

    logger.debug("Connect to the TMP science database")
    df = load_parquet_files([path.format("science") for path in paths])
    df_raw_cols = load_parquet_files([path.format("raw") for path in paths]).columns

    logger.debug("Retrieve time-series information")
    to_expand = [
        "jd",
        "fid",
        "magpsf",
        "sigmapsf",
        "magnr",
        "sigmagnr",
        "magzpsci",
        "isdiffpos",
        "diffmaglim",
    ]

    logger.debug("Append temp columns with historical + current measurements")
    prefix = "c"
    for colname in to_expand:
        df = concat_col(df, colname, prefix=prefix)

    # quick fix for https://github.com/astrolabsoftware/fink-broker/issues/457
    for colname in to_expand:
        df = df.withColumnRenamed("c" + colname, "c" + colname + "c")

    df = df.withColumn("cstampDatac", df["cutoutScience.stampData"])

    for userfilter in userfilters:
        _LOG.debug("Apply user-defined filter %s", userfilter)
        df_tmp = apply_user_defined_filter(df, userfilter, _LOG)

        df_tmp = df_tmp.select(df_raw_cols)

        nb_alert = df_tmp.count()
        _LOG.info(f"nb alerts before threshold: {nb_alert}")

        df_tmp = df_tmp.limit(10).repartition(nb_alert)

        df_tmp.write.format("avro").option("compression", "uncompressed").save(
            f"test_ci_data/{userfilter}"
        )


if __name__ == "__main__":
    main()
