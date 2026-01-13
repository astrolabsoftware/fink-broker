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
"""Push image data metadata to an HBase table"""

import argparse


from fink_broker.common.logging_utils import get_fink_logger, inspect_application
from fink_broker.common.parser import getargs

from fink_broker.common.spark_utils import init_sparksession
from fink_broker.common.spark_utils import list_hdfs_files


from fink_broker.rubin.hbase_utils import ingest_cutout_metadata


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(
        name="image_archival_{}".format(args.night), shuffle_partitions=2
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

    ingest_cutout_metadata(
        paths,
        table_name=args.science_db_name + ".cutouts",
        catfolder=args.science_db_catalogs,
        npartitions=1000,
        streaming=False,
    )


if __name__ == "__main__":
    main()
