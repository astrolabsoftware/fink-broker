#!/usr/bin/env python
# Copyright 2020-2024 AstroLab Software
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

import argparse

from fink_broker.parser import getargs
from fink_broker.sparkUtils import init_sparksession, load_parquet_files

from fink_broker.hbase_utils import push_full_df_to_hbase
from fink_broker.sparkUtils import list_hdfs_files

from fink_broker.logging_utils import get_fink_logger, inspect_application


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(
        name="science_archival_{}".format(args.night), shuffle_partitions=2
    )

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    # Connect to the aggregated science database
    folder = "{}/science/year={}/month={}/day={}".format(
        args.agg_data_prefix, args.night[:4], args.night[4:6], args.night[6:8]
    )

    paths = list_hdfs_files(folder)
    npath = len(paths)
    logger.info("{} parquet detected".format(npath))

    # Row key
    row_key_name = "objectId_jd"
    n_alerts = 0
    for index, path in enumerate(paths):
        df = load_parquet_files(path)
        n_alerts_parquet = df.count()
        logger.info(
            "Pushing {}/{} parquet to HBase ({} alerts)".format(
                index + 1, npath, n_alerts_parquet
            )
        )
        n_alerts += n_alerts_parquet

        # Drop partitioning columns
        df = df.drop("year").drop("month").drop("day")

        # push data to HBase
        push_full_df_to_hbase(
            df,
            row_key_name=row_key_name,
            table_name=args.science_db_name,
            catalog_name=args.science_db_catalogs,
        )
    logger.info("{} alerts pushed to HBase".format(n_alerts))


if __name__ == "__main__":
    main()
