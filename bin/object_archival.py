#!/usr/bin/env python
# Copyright 2022 AstroLab Software
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
"""Push object data to the science portal (HBase table)

1. load object ID data from HBase table
2. groupby object ID
4. Push data (single shot)
"""

import pyspark.sql.functions as F

import os
import argparse

from fink_broker.parser import getargs
from fink_broker.spark_utils import init_sparksession
from fink_broker.hbase_utils import push_to_hbase, load_hbase_data
from fink_broker.logging_utils import get_fink_logger, inspect_application

from fink_utils.hbase.utils import load_hbase_catalog_as_dict
from fink_utils.hbase.utils import select_columns_in_catalog
from fink_utils.hbase.utils import group_by_key


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(
        name="object_archival_{}".format(args.night), shuffle_partitions=2
    )

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    # Table 1: Candidate SSO

    # Load data from HBase
    catalog, rowkey = load_hbase_catalog_as_dict(
        os.path.join(
            args.science_db_catalogs, "{}.sso_cand.json".format(args.science_db_name)
        )
    )
    _, catalog_small = select_columns_in_catalog(catalog, cols=["jd_trajectory_id"])
    df = load_hbase_data(catalog_small, rowkey)

    # group by
    df_grouped = group_by_key(df, "jd_trajectory_id", position=1)
    df_grouped = df_grouped.withColumnRenamed("id", "trajectory_id")

    # Definitions
    index_row_key_name = "count_trajectory_id"
    index_name = ".ssocand_oid"

    # add the rowkey -- and select only it
    df_grouped = df_grouped.withColumn(
        index_row_key_name, F.concat_ws("_", *["count", "trajectory_id"])
    ).select(index_row_key_name)

    cf = {i: "d" for i in df_grouped.columns}

    push_to_hbase(
        df=df_grouped,
        table_name=args.science_db_name + index_name,
        rowkeyname=index_row_key_name,
        cf=cf,
        catfolder=args.science_db_catalogs,
    )

    # Table 2: Known SSO

    # Load data from HBase
    catalog, rowkey = load_hbase_catalog_as_dict(
        os.path.join(
            args.science_db_catalogs, "{}.ssnamenr.json".format(args.science_db_name)
        )
    )
    _, catalog_small = select_columns_in_catalog(catalog, cols=["ssnamenr_jd"])
    df = load_hbase_data(catalog_small, rowkey)

    # group by
    df_grouped = group_by_key(df, "ssnamenr_jd", position=1)
    df_grouped = df_grouped.withColumnRenamed("id", "ssnamenr")

    # Definitions
    index_row_key_name = "count_ssnamenr"
    index_name = ".sso_oid"

    # add the rowkey -- and select only it
    df_grouped = df_grouped.withColumn(
        index_row_key_name, F.concat_ws("_", *["count", "ssnamenr"])
    ).select(index_row_key_name)

    cf = {i: "d" for i in df_grouped.columns}

    push_to_hbase(
        df=df_grouped,
        table_name=args.science_db_name + index_name,
        rowkeyname=index_row_key_name,
        cf=cf,
        catfolder=args.science_db_catalogs,
    )


if __name__ == "__main__":
    main()
