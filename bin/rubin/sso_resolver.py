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

import argparse

from fink_broker.common.spark_utils import init_sparksession
from fink_broker.common.hbase_utils import add_row_key, push_to_hbase
from fink_broker.common.parser import getargs
from fink_broker.common.logging_utils import init_logger
from fink_broker.common.spark_utils import load_parquet_files
from fink_broker.common.hbase_utils import salt_from_last_digits
from fink_broker.common.spark_utils import list_hdfs_files


def main():
    """Table to resolve ssObjectId into various designations"""
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    logger = init_logger(args.log_level)

    # ssObjectId --> designation(s)
    cols_row_key_name = ["salt", "ssObjectId"]
    row_key_name = "_".join(cols_row_key_name)
    index_name = ".sso_resolver"

    # Initialise Spark session
    init_sparksession(name="sso_resolver_{}".format(args.night), shuffle_partitions=2)

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

    # Keep only SSO
    df = df.filter(df["mpc_orbits"].isNotNull())

    # Add salt
    df = salt_from_last_digits(
        df, colname="{}.{}".format("ssSource", "ssObjectId"), npartitions=1000
    )

    # Select wanted columns
    cols_rubin = [
        "mpc_orbits.packed_primary_provisional_designation",
        "mpc_orbits.unpacked_primary_provisional_designation",
        "ssSource.ssObjectId",
        "diaSource.diaSourceId",
    ]
    cols_fink = ["salt"]

    df = df.select(cols_rubin + cols_fink)

    df = add_row_key(df, row_key_name=row_key_name, cols=cols_row_key_name)

    # Get rid of salt for HBase
    df = df.drop("salt")

    cf = {i: "r" for i in df.columns}

    push_to_hbase(
        df=df,
        table_name=args.science_db_name + index_name,
        rowkeyname=row_key_name,
        cf=cf,
        catfolder=args.science_db_catalogs,
    )


if __name__ == "__main__":
    main()
