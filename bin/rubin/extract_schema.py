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
"""Extract AVRO schemas from a DataFrame"""

import argparse

from fink_broker.common.parser import getargs
from fink_broker.common.spark_utils import init_sparksession
from fink_broker.common.logging_utils import get_fink_logger, inspect_application

from fink_broker.rubin.spark_utils import get_schema_from_parquet
from fink_broker.common.spark_utils import list_hdfs_files
from fink_broker.common.spark_utils import load_parquet_files
from fink_utils.spark import schema_converter


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(
        name="extract_schema_{}".format(args.night), shuffle_partitions=200
    )

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    # Connect to the aggregated science database
    folder = "{}/science/year={}/month={}/day={}".format(
        args.agg_data_prefix, args.night[:4], args.night[4:6], args.night[6:8]
    )

    # Get list of files
    paths = list_hdfs_files(folder)

    if len(paths) == 0:
        logger.warning("No parquet found at {}".format(folder))

        import sys

        sys.exit()

    df = load_parquet_files(paths)

    # LSST versions
    major_version, minor_version = get_schema_from_parquet(paths)

    # Fink versions
    fink_versions = (
        df
        .select(["fink_broker_version", "fink_science_version"])
        .limit(1)
        .collect()[0]
        .asDict()
    )

    # extract full schema
    full_schema = schema_converter.to_avro(df.schema)

    with open(
        "lsst{}.{}_fink_{}_{}_full_schema.avsc".format(
            major_version,
            minor_version,
            fink_versions["fink_broker_version"],
            fink_versions["fink_science_version"],
        ),
        "w",
    ) as f:
        f.write(full_schema)


if __name__ == "__main__":
    main()
