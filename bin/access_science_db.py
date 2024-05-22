#!/usr/bin/env python
# Copyright 2019-2021 AstroLab Software
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
"""Access the science database, and read alerts. HBase must be installed."""

import os
import argparse
import json

from fink_broker.parser import getargs
from fink_broker.sparkUtils import init_sparksession
from fink_broker.logging_utils import get_fink_logger, inspect_application


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Grab the running Spark Session,
    # otherwise create it.
    spark = init_sparksession(name="readingScienceDB", shuffle_partitions=2)

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    with open(
        os.path.join(args.science_db_catalogs, "{}.json".format(args.science_db_name))
    ) as f:
        catalog = json.load(f)

    catalog_dic = json.loads(catalog)

    df = (
        spark.read.option("catalog", catalog)
        .format("org.apache.hadoop.hbase.spark")
        .option("hbase.spark.use.hbasecontext", False)
        .load()
    )

    print("Number of entries in {}: ".format(catalog_dic["table"]["name"]), df.count())

    print(
        "Number of distinct objects in {}: ".format(catalog_dic["table"]["name"]),
        df.select("objectId").distinct().count(),
    )


if __name__ == "__main__":
    main()
