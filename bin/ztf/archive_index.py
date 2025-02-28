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

"""Push science data to the science portal (HBase table)

1. Use the Alert data that is stored in the Science TMP database (Parquet)
2. Extract relevant information from alerts
3. Construct HBase catalog
4. Push data (single shot)
"""

import argparse

import pyspark.sql.functions as F

from fink_broker.ztf.parser import getargs
from fink_broker.common.spark_utils import ang2pix
from fink_broker.ztf.hbase_utils import push_to_hbase, add_row_key
from fink_broker.ztf.hbase_utils import assign_column_family_names
from fink_broker.ztf.hbase_utils import load_ztf_index_cols
from fink_broker.ztf.hbase_utils import load_ztf_crossmatch_cols
from fink_broker.ztf.hbase_utils import select_relevant_columns
from fink_broker.ztf.hbase_utils import bring_to_current_schema
from fink_broker.common.spark_utils import init_sparksession, load_parquet_files
from fink_broker.common.logging_utils import get_fink_logger, inspect_application

from fink_filters.classification import extract_fink_classification
from fink_utils.spark.utils import check_status_last_prv_candidates


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # construct the index view
    index_row_key_name = args.index_table
    columns = index_row_key_name.split("_")
    index_name = "." + columns[0]

    # Initialise Spark session
    spark = init_sparksession(
        name="index_archival_{}_{}".format(args.index_table, args.night),
        shuffle_partitions=2,
    )

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    # Connect to the aggregated science database
    path = "{}/science/year={}/month={}/day={}".format(
        args.agg_data_prefix, args.night[:4], args.night[4:6], args.night[6:8]
    )
    data = load_parquet_files(path)

    # Drop partitioning columns
    data = data.drop("year").drop("month").drop("day")

    # Check all columns exist, fill if necessary, and cast data
    df_flat, cols_i, cols_d = bring_to_current_schema(data)

    # Assign each column to a specific column family
    # This is independent from the final structure
    cf = assign_column_family_names(df_flat, cols_i, cols_d)

    # Restrict the input DataFrame to the subset of wanted columns,
    # except for tables containing uppervalid & upper limit data
    if "upper" not in args.index_table:
        df = df_flat

    # Load common cols (casted)
    common_cols = load_ztf_index_cols()

    if columns[0].startswith("pixel"):
        nside = int(columns[0].split("pixel")[1])
        xmatch_cols = load_ztf_crossmatch_cols()

        df_index = df.withColumn(
            columns[0], ang2pix(df["ra"], df["dec"], F.lit(nside))
        ).withColumn(
            "classification",
            extract_fink_classification(
                df["cdsxmatch"],
                df["roid"],
                df["mulens"],
                df["snn_snia_vs_nonia"],
                df["snn_sn_vs_all"],
                df["rf_snia_vs_nonia"],
                df["ndethist"],
                df["drb"],
                df["classtar"],
                df["jd"],
                df["jdstarthist"],
                df["rf_kn_vs_nonkn"],
                df["tracklet"],
            ),
        )

        # Update cf with added column
        cf.update({"classification": "d"})

        # Row key
        df_index = add_row_key(df_index, row_key_name=index_row_key_name, cols=columns)

        df_index = select_relevant_columns(
            df_index,
            cols=xmatch_cols + ["classification"],
            row_key_name=index_row_key_name,
        )
    elif columns[0] == "class":
        df_index = df.withColumn(
            "class",
            extract_fink_classification(
                df["cdsxmatch"],
                df["roid"],
                df["mulens"],
                df["snn_snia_vs_nonia"],
                df["snn_sn_vs_all"],
                df["rf_snia_vs_nonia"],
                df["ndethist"],
                df["drb"],
                df["classtar"],
                df["jd"],
                df["jdstarthist"],
                df["rf_kn_vs_nonkn"],
                df["tracklet"],
            ),
        )
        # Row key
        df_index = add_row_key(df_index, row_key_name=index_row_key_name, cols=columns)
        df_index = select_relevant_columns(
            df_index, cols=common_cols, row_key_name=index_row_key_name
        )
    elif columns[0] == "ssnamenr":
        # Flag only objects with likely counterpart in MPC
        df_index = df.filter(df["roid"] == 3)
        # Row key
        df_index = add_row_key(df_index, row_key_name=index_row_key_name, cols=columns)
        df_index = select_relevant_columns(
            df_index, cols=common_cols, row_key_name=index_row_key_name
        )
    elif columns[0] == "tracklet":
        # For data < 2021-08-10, no tracklet means ''
        # For data >= 2021-08-10, no tracklet means 'null'
        df_index = df.filter(df["tracklet"] != "null").filter(df["tracklet"] != "")

        # Row key
        df_index = add_row_key(df_index, row_key_name=index_row_key_name, cols=columns)
        df_index = select_relevant_columns(
            df_index, cols=common_cols, row_key_name=index_row_key_name
        )
    elif columns[0] == "upper":
        # select only data for which there are new recent upper limits
        data = check_status_last_prv_candidates(data, status="upper")
        data = data.filter("upper")

        df = data.select(
            F.col("objectId").cast("string").alias("objectId"),
            F.col("prv_candidates.jd").cast("array<double>").alias("jd"),
            F.col("prv_candidates.fid").cast("array<int>").alias("fid"),
            F.col("prv_candidates.magpsf").cast("array<float>").alias("magpsf"),
            F.col("prv_candidates.sigmapsf").cast("array<float>").alias("sigmapsf"),
            F.col("prv_candidates.diffmaglim").cast("array<float>").alias("diffmaglim"),
        )

        index_row_key_name = "objectId_jd"

        # explode
        df_ex = (
            df.withColumn(
                "tmp", F.arrays_zip("magpsf", "sigmapsf", "diffmaglim", "jd", "fid")
            )
            .withColumn("tmp", F.explode("tmp"))
            .select(
                F.concat_ws("_", "objectId", "tmp.jd").alias(index_row_key_name),
                "objectId",
                F.col("tmp.jd"),
                F.col("tmp.fid"),
                F.col("tmp.magpsf"),
                F.col("tmp.sigmapsf"),
                F.col("tmp.diffmaglim"),
            )
        )

        # take only upper limits within remaining historical data
        df_index = df_ex.filter(~df_ex["magpsf"].isNotNull())
        # drop NaN columns
        df_index = df_index.drop(*["magpsf", "sigmapsf"])
    elif columns[0] == "uppervalid":
        # select only data for which there are new recent low quality data
        data = check_status_last_prv_candidates(data, status="uppervalid")
        data = data.filter("uppervalid")

        df = data.select(
            F.col("objectId").cast("string").alias("objectId"),
            F.col("prv_candidates.jd").cast("array<double>").alias("jd"),
            F.col("prv_candidates.fid").cast("array<int>").alias("fid"),
            F.col("prv_candidates.magpsf").cast("array<float>").alias("magpsf"),
            F.col("prv_candidates.sigmapsf").cast("array<float>").alias("sigmapsf"),
            F.col("prv_candidates.diffmaglim").cast("array<float>").alias("diffmaglim"),
            F.col("prv_candidates.magnr").cast("array<float>").alias("magnr"),
            F.col("prv_candidates.sigmagnr").cast("array<float>").alias("sigmagnr"),
            F.col("prv_candidates.isdiffpos").cast("array<string>").alias("isdiffpos"),
            F.col("prv_candidates.distnr").cast("array<float>").alias("distnr"),
            F.col("prv_candidates.rb").cast("array<float>").alias("rb"),
            F.col("prv_candidates.nbad").cast("array<int>").alias("nbad"),
        )

        index_row_key_name = "objectId_jd"

        # explode
        df_ex = (
            df.withColumn(
                "tmp",
                F.arrays_zip(
                    "magpsf",
                    "sigmapsf",
                    "diffmaglim",
                    "jd",
                    "fid",
                    "magnr",
                    "sigmagnr",
                    "isdiffpos",
                    "distnr",
                    "rb",
                    "nbad",
                ),
            )
            .withColumn("tmp", F.explode("tmp"))
            .select(
                F.concat_ws("_", "objectId", "tmp.jd").alias(index_row_key_name),
                "objectId",
                F.col("tmp.jd"),
                F.col("tmp.fid"),
                F.col("tmp.magpsf"),
                F.col("tmp.sigmapsf"),
                F.col("tmp.diffmaglim"),
                F.col("tmp.magnr"),
                F.col("tmp.sigmagnr"),
                F.col("tmp.isdiffpos"),
                F.col("tmp.distnr"),
                F.col("tmp.rb"),
                F.col("tmp.nbad"),
            )
        )

        # take only noisy measurements from the history
        f1 = (df_ex["rb"] >= 0.55) & (df_ex["nbad"] == 0)
        cond = ~f1 & df_ex["magpsf"].isNotNull()
        df_index = df_ex.filter(cond)

        # Remove unused columns
        df_index = df_index.drop(*["rb", "nbad"])
    elif columns[0] == "tns":
        # Flag only objects with counterpart in TNS
        df_index = df.filter(df["tns"] != "")

        # Row key
        df_index = add_row_key(df_index, row_key_name=index_row_key_name, cols=columns)
        df_index = select_relevant_columns(
            df_index, cols=common_cols, row_key_name=index_row_key_name
        )
    else:
        # Row key
        df = add_row_key(df, row_key_name=index_row_key_name, cols=columns)

        df_index = select_relevant_columns(
            df, cols=common_cols, row_key_name=index_row_key_name
        )

    push_to_hbase(
        df=df_index,
        table_name=args.science_db_name + index_name,
        rowkeyname=index_row_key_name,
        cf=cf,
        catfolder=args.science_db_catalogs,
    )


if __name__ == "__main__":
    main()
