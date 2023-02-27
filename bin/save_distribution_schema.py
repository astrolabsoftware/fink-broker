#!/usr/bin/env python
# Copyright 2020-2023 AstroLab Software
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
"""Save distribution schema on disk
"""
import argparse
from time import time

from fink_broker.parser import getargs
from fink_broker.sparkUtils import init_sparksession, load_parquet_files
from fink_broker.loggingUtils import get_fink_logger, inspect_application
from fink_broker.distributionUtils import save_and_load_schema

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(name="save_schema_{}".format(args.night), shuffle_partitions=2)

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    # Connect to the aggregated science database
    year = args.night[:4]
    month = args.night[4:6]
    day = args.night[6:8]

    print('Processing {}/{}/{}'.format(year, month, day))

    input_science = '{}/science/year={}/month={}/day={}'.format(
        args.agg_data_prefix, year, month, day)
    df = load_parquet_files(input_science)

    # Drop partitioning columns
    df = df.drop('year').drop('month').drop('day')

    # Cast fields to ease the distribution
    cnames = df.columns
    cnames[cnames.index('timestamp')] = 'cast(timestamp as string) as timestamp'
    cnames[cnames.index('cutoutScience')] = 'struct(cutoutScience.*) as cutoutScience'
    cnames[cnames.index('cutoutTemplate')] = 'struct(cutoutTemplate.*) as cutoutTemplate'
    cnames[cnames.index('cutoutDifference')] = 'struct(cutoutDifference.*) as cutoutDifference'
    cnames[cnames.index('prv_candidates')] = 'explode(array(prv_candidates)) as prv_candidates'
    cnames[cnames.index('candidate')] = 'struct(candidate.*) as candidate'
    cnames[cnames.index('lc_features_g')] = 'struct(lc_features_g.*) as lc_features_g'
    cnames[cnames.index('lc_features_r')] = 'struct(lc_features_r.*) as lc_features_r'

    df_kafka = df.selectExpr(cnames)

    path_for_avro = 'new_schema_{}.avro'.format(time())

    save_and_load_schema(df_kafka, path_for_avro, fs=args.fs)


if __name__ == "__main__":
    main()
