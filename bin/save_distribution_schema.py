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
"""Save distribution schema on disk"""

import argparse
from time import time
import subprocess
import glob
import json

from fink_broker.parser import getargs
from fink_broker.sparkUtils import init_sparksession, load_parquet_files
from fink_broker.avroUtils import readschemafromavrofile
from fink_broker.loggingUtils import get_fink_logger, inspect_application


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(
        name="save_schema_{}".format(args.night), shuffle_partitions=2
    )

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    # Connect to the aggregated science database
    year = args.night[:4]
    month = args.night[4:6]
    day = args.night[6:8]

    print("Processing {}/{}/{}".format(year, month, day))

    input_science = "{}/science/year={}/month={}/day={}".format(
        args.agg_data_prefix, year, month, day
    )
    df = load_parquet_files(input_science)

    # Drop partitioning columns
    df = df.drop("year").drop("month").drop("day")

    # Cast fields to ease the distribution
    cnames = df.columns
    cnames[cnames.index("timestamp")] = "cast(timestamp as string) as timestamp"
    cnames[cnames.index("cutoutScience")] = "struct(cutoutScience.*) as cutoutScience"
    cnames[cnames.index("cutoutTemplate")] = (
        "struct(cutoutTemplate.*) as cutoutTemplate"
    )
    cnames[cnames.index("cutoutDifference")] = (
        "struct(cutoutDifference.*) as cutoutDifference"
    )
    cnames[cnames.index("prv_candidates")] = (
        "explode(array(prv_candidates)) as prv_candidates"
    )
    cnames[cnames.index("candidate")] = "struct(candidate.*) as candidate"

    df_kafka = df.selectExpr(cnames)

    path_for_avro = "new_schema_{}.avro".format(time())
    df_kafka.limit(1).write.format("avro").save(path_for_avro)

    # retrieve data on local disk
    subprocess.run(["hdfs", "dfs", "-get", path_for_avro])

    # Read the avro schema from .avro file
    avro_file = glob.glob(path_for_avro + "/part*")[0]
    avro_schema = readschemafromavrofile(avro_file)

    # Write the schema to a file for decoding Kafka messages
    with open("schemas/{}".format(path_for_avro.replace(".avro", ".avsc")), "w") as f:
        json.dump(avro_schema, f, indent=2)


if __name__ == "__main__":
    main()
