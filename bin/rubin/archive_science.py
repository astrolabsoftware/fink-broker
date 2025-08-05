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
4. Push data
"""

import argparse

from fink_broker.common.parser import getargs
from fink_broker.common.spark_utils import init_sparksession
from fink_broker.common.spark_utils import list_hdfs_files
from fink_broker.common.logging_utils import get_fink_logger, inspect_application
from fink_broker.rubin.hbase_utils import incremental_ingestion_with_salt
from fink_broker.rubin.hbase_utils import deduplicate_ingestion_with_salt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(
        name="science_archival_{}".format(args.night), shuffle_partitions=200
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

    # Get version from the alert directly rather than from remote
    # This prevents reprocessing to use wrong schema
    pqs = [i for i in paths if i.endswith(".parquet")]
    schema_version = (
        spark.read.format("parquet")
        .load(pqs[0])
        .select("lsst_schema_version")
        .limit(1)
        .collect()[0][0]
    )
    major_version, minor_version = schema_version.split("lsst.v")[1].split("_")

    # FIXME: should be CLI arg
    nfiles = 100
    npartitions = 1000

    logger.info(
        "{} parquet detected ({} loops to perform)".format(
            len(paths), int(len(paths) / nfiles) + 1
        )
    )

    # diaObject
    row_key_name = "salt_diaObjectId"
    table_name = "{}.{}".format(args.science_db_name, "diaObject")
    n_alerts_diaobject = deduplicate_ingestion_with_salt(
        paths=paths,
        table_name=table_name,
        row_key_name=row_key_name,
        catfolder=args.science_db_catalogs,
        major_version=major_version,
        minor_version=minor_version,
        npartitions=npartitions,
    )
    logger.info(
        "{} alerts pushed to HBase for table {}".format(n_alerts_diaobject, table_name)
    )

    # diaSource
    row_key_name = "salt_diaObjectId_midpointMjdTai"
    table_name = "{}.{}".format(args.science_db_name, "diaSource")
    n_alerts_diasource = incremental_ingestion_with_salt(
        paths=paths,
        table_name=table_name,
        row_key_name=row_key_name,
        catfolder=args.science_db_catalogs,
        major_version=major_version,
        minor_version=minor_version,
        nfiles=nfiles,
        npartitions=npartitions,
    )
    logger.info(
        "{} alerts pushed to HBase for table {}".format(n_alerts_diasource, table_name)
    )


if __name__ == "__main__":
    main()
