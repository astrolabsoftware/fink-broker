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
import pyspark.sql.functions as F

import argparse
import os

from fink_tns.utils import download_catalog
from fink_broker.common.spark_utils import init_sparksession
from fink_broker.common.hbase_utils import add_row_key, push_to_hbase
from fink_broker.common.parser import getargs
from fink_broker.common.logging_utils import init_logger
from fink_broker.common.hbase_utils import format_tns_for_hbase
from fink_broker.common.spark_utils import save_tns_parquet_on_disk


def main():
    """Download the TNS catalog, and load it in HBase"""
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    logger = init_logger(args.log_level)

    if os.environ.get("TNS_API_MARKER") is None:
        logger.warning("TNS_API_MARKER is not defined. Exiting.")

        # you do not want the rest of the pipeline to crash, e.g. in CI
        return 0

    # construct the index view 'fullname_internalname'
    index_row_key_name = "salt_fullname_internalname"
    columns = index_row_key_name.split("_")
    index_name = ".tns_resolver"

    # Initialise Spark session
    spark = init_sparksession(
        name="tns_resolver_{}".format(args.night), shuffle_partitions=2
    )

    with open(os.environ["TNS_API_MARKER"]) as f:
        tns_marker = f.read().replace("\n", "")

    pdf_tns = download_catalog(os.environ["TNS_API_KEY"], tns_marker)

    # Make a Spark DataFrame
    df_index = spark.createDataFrame(format_tns_for_hbase(pdf_tns, with_salt=True))

    df_index = add_row_key(df_index, row_key_name=index_row_key_name, cols=columns)

    # make the rowkey lower case
    df_index = df_index.withColumn(index_row_key_name, F.lower(index_row_key_name))

    # Fink added value
    cf = {i: "f" for i in df_index.columns}

    push_to_hbase(
        df=df_index,
        table_name=args.science_db_name + index_name,
        rowkeyname=index_row_key_name,
        cf=cf,
        catfolder=args.science_db_catalogs,
    )

    save_tns_parquet_on_disk(pdf_tns, args.tns_raw_output)


if __name__ == "__main__":
    main()
