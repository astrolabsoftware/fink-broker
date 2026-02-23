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


from fink_broker.common.spark_utils import init_sparksession
from fink_broker.common.spark_utils import list_hdfs_files

from fink_broker.common.logging_utils import get_fink_logger, inspect_application
from fink_broker.common.parser import getargs

from fink_broker.rubin.spark_utils import get_schema_from_parquet
from fink_broker.rubin.hbase_utils import ingest_pixels


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # construct the index view
    # FIXME: construct row key from individual names instead
    index_row_key_name = args.index_table
    columns = index_row_key_name.split("_")

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

    if columns[0].startswith("pixel"):
        # FIXME: should be CLI arg. This is fixed when creating HBase tables
        npartitions = 1000

        # Pixels
        ingest_pixels(
            paths=paths,
            table_name=args.science_db_name + "." + columns[0],
            catfolder=args.science_db_catalogs,
            major_version=major_version,
            minor_version=minor_version,
            npartitions=npartitions,
            streaming=False,
        )

    else:
        logger.warning("{} is not a supported index name.".format(columns[0]))
        sys.exit(1)


if __name__ == "__main__":
    main()
