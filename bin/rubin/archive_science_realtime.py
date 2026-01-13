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

"""Push science data to HBase in real time"""

import time
import argparse

from fink_broker.common.parser import getargs
from fink_broker.common.spark_utils import init_sparksession
from fink_broker.common.logging_utils import get_fink_logger, inspect_application

from fink_broker.rubin.hbase_utils import ingest_source_data
from fink_broker.rubin.hbase_utils import ingest_object_data
from fink_broker.rubin.spark_utils import get_schema_from_parquet


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(
        name="archive_science_realtime_{}".format(args.night), shuffle_partitions=200
    )

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    # Connect to the aggregated science database
    path = args.online_data_prefix + "/science/{}".format(args.night)
    checkpointpath_hbase = args.online_data_prefix + "/hbase_checkpoint/{}".format(
        args.night
    )

    major_version, minor_version = get_schema_from_parquet(path)

    # FIXME: should be CLI arg. This is fixed when creating HBase tables
    npartitions = 1000

    queries = []
    # diaObject
    _, _, hbase_query_static = ingest_object_data(
        kind="static",
        paths=path,
        catfolder=args.science_db_catalogs,
        major_version=major_version,
        minor_version=minor_version,
        npartitions=npartitions,
        streaming=True,
        checkpoint_path=checkpointpath_hbase + "/static",
    )
    queries.append(hbase_query_static)

    # ssObject/mpc_orbits
    if major_version >= 10:
        _, _, hbase_query_sso = ingest_object_data(
            kind="sso",
            paths=path,
            catfolder=args.science_db_catalogs,
            major_version=major_version,
            minor_version=minor_version,
            npartitions=npartitions,
            streaming=True,
            checkpoint_path=checkpointpath_hbase + "/sso",
        )
        queries.append(hbase_query_sso)
    else:
        logger.warning(
            "Version {} detected. Skipping SSO injection".format(major_version)
        )

    # diaSource (static)
    _, _, hbase_query_static_source = ingest_source_data(
        kind="static",
        paths=path,
        catfolder=args.science_db_catalogs,
        major_version=major_version,
        minor_version=minor_version,
        npartitions=npartitions,
        streaming=True,
        checkpoint_path=checkpointpath_hbase + "/static_source",
    )
    queries.append(hbase_query_static_source)

    # diaSource (SSO)
    if major_version >= 10:
        _, _, hbase_query_sso_source = ingest_source_data(
            kind="sso",
            paths=path,
            catfolder=args.science_db_catalogs,
            major_version=major_version,
            minor_version=minor_version,
            npartitions=npartitions,
            streaming=True,
            checkpoint_path=checkpointpath_hbase + "/sso_source",
        )
        queries.append(hbase_query_sso_source)
    else:
        logger.warning(
            "Version {} detected. Skipping SSO injection".format(major_version)
        )

    if args.exit_after is not None:
        logger.debug("Keep the Streaming running until something or someone ends it!")
        remaining_time = args.exit_after
        remaining_time = remaining_time if remaining_time > 0 else 0
        time.sleep(remaining_time)
        for hbase_query in queries:
            hbase_query.stop()
        logger.info("Exiting the archive_science_streaming service normally...")
    else:
        logger.debug("Wait for the end of queries")
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
