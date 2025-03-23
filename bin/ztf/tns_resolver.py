#!/usr/bin/env python
# Copyright 2023-2024 AstroLab Software
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

import pandas as pd
import argparse
import os

from fink_tns.utils import download_catalog
from fink_broker.common.spark_utils import init_sparksession
from fink_broker.ztf.hbase_utils import add_row_key, push_to_hbase
from fink_broker.common.parser import getargs


def format_tns_for_hbase(pdf: pd.DataFrame) -> pd.DataFrame:
    """Format the raw TNS data for HBase ingestion"""
    # Add new or rename columns
    pdf["fullname"] = pdf["name_prefix"] + " " + pdf["name"]
    pdf["internalname"] = pdf["internal_names"]

    # Apply quality cuts
    mask = pdf["internalname"].apply(lambda x: (x is not None) and (x == x))  # NOSONAR
    pdf_val = pdf[mask]
    pdf_val["type"] = pdf_val["type"].astype("str")

    pdf_val["internalname"] = pdf_val["internalname"].apply(
        lambda x: [i.strip() for i in x.split(",")]
    )

    pdf_explode = pdf_val.explode("internalname")

    # Select columns of interest -- and create a Spark DataFrame
    cols = ["fullname", "ra", "declination", "type", "redshift", "internalname"]

    return pdf_explode[cols]


def main():
    """Download the TNS catalog, and load it in HBase"""
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # construct the index view 'fullname_internalname'
    index_row_key_name = "fullname_internalname"
    columns = index_row_key_name.split("_")
    index_name = ".tns_resolver"

    # Initialise Spark session
    spark = init_sparksession(
        name="tns_resolver_{}".format(args.night), shuffle_partitions=2
    )

    with open("{}/tns_marker.txt".format(args.tns_folder)) as f:
        tns_marker = f.read().replace("\n", "")

    pdf_tns = download_catalog(os.environ["TNS_API_KEY"], tns_marker)

    # Push to HBase
    df_index = spark.createDataFrame(format_tns_for_hbase(pdf_tns))

    df_index = add_row_key(df_index, row_key_name=index_row_key_name, cols=columns)

    # make the rowkey lower case
    df_index = df_index.withColumn(index_row_key_name, F.lower(index_row_key_name))

    cf = {i: "d" for i in df_index.columns}

    push_to_hbase(
        df=df_index,
        table_name=args.science_db_name + index_name,
        rowkeyname=index_row_key_name,
        cf=cf,
        catfolder=args.science_db_catalogs,
    )

    # Save raw data
    pdf_tns.to_parquet("{}/tns_raw.parquet".format(args.tns_raw_output))

    # Filter TNS confirmed data
    f1 = ~pdf_tns["type"].isna()
    pdf_tns_filt = pdf_tns[f1]
    pdf_tns_filt["type"] = pdf_tns_filt["type"].apply(lambda x: "(TNS) {}".format(x))

    pdf_tns_filt.to_parquet("{}/tns.parquet".format(args.tns_raw_output))


if __name__ == "__main__":
    main()
