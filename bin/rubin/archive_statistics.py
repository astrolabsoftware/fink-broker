#!/usr/bin/env python
# Copyright 2025 AstroLab Software
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
"""Compute statistics for a given observing night"""

import argparse

import pandas as pd


from fink_broker.common.spark_utils import init_sparksession
from fink_broker.common.parser import getargs
from fink_broker.common.logging_utils import get_fink_logger
from fink_broker.common.hbase_utils import push_to_hbase


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(
        name="statistics_{}".format(args.night), shuffle_partitions=2
    )

    # Logger to print useful debug statements
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    year = args.night[:4]
    month = args.night[4:6]
    day = args.night[6:8]

    logger.info("Statistics for {}/{}/{}".format(year, month, day))

    folder = "{}/science/year={}/month={}/day={}".format(
        args.agg_data_prefix, year, month, day
    )

    df = spark.read.format("parquet").load(folder)

    out_dic = {}

    # Number of alerts
    out_dic["alerts"] = df.count()

    # Versions
    version_fields = [
        "fink_broker_version",
        "fink_science_version",
        "lsst_schema_version",
    ]
    for version_field in version_fields:
        out_dic[version_field] = df.select(version_field).limit(1).collect()[0][0]

    # per band
    perband = df.groupby("diaSource.band").count().collect()

    # Create a dictionary from list a for quick lookup
    a_dict = {item["band"]: item["count"] for item in perband}

    all_bands = ["u", "g", "r", "i", "z", "y"]
    container = [{"band": i, "count": 0} for i in all_bands]
    for item in container:
        if item["band"] in a_dict:
            item["count"] = a_dict[item["band"]]
        out_dic.update({"alerts_{}".format(item["band"]): item["count"]})

    # Number of objects (removing asteroids)
    out_dic["objects"] = df.select("diaObject.diaObjectId").distinct().count() - 1

    # SSO, first, cataloged
    out_dic["is_sso"] = df.filter(df["pred.is_sso"]).count()
    out_dic["is_first"] = df.filter(df["pred.is_first"]).count()
    out_dic["is_cataloged"] = df.filter(df["pred.is_cataloged"]).count()

    # TNS
    tns = (
        df
        .filter(df["xm.tns_type"].isNotNull())
        .groupBy("xm.tns_type")
        .count()
        .collect()
    )
    if len(tns) > 0:
        out_dic["in_tns"] = sum(i["count"] for i in tns)
    else:
        out_dic["in_tns"] = 0

    # Various Flags
    out_dic["glint_trail"] = df.select("diaSource.glint_trail").count()
    out_dic["isDipole"] = df.select("diaSource.isDipole").count()
    out_dic["pixelFlags_cr"] = df.select("diaSource.pixelFlags_cr").count()
    out_dic["pixelFlags_saturated"] = df.select(
        "diaSource.pixelFlags_saturated"
    ).count()
    out_dic["pixelFlags_streak"] = df.select("diaSource.pixelFlags_streak").count()

    # Number of visits
    out_dic["visits"] = df.select("diaSource.midpointMjdTai").distinct().count()

    out_dic["night"] = "{}".format(args.night)

    # make a Spark DataFrame
    pdf = pd.DataFrame([out_dic])
    df_hbase = spark.createDataFrame(pdf)

    # rowkey is the night YYYYMMDD
    index_row_key_name = "night"

    # column families
    cf = {i: "f" for i in df_hbase.columns}

    push_to_hbase(
        df=df_hbase,
        table_name="rubin.statistics",
        rowkeyname=index_row_key_name,
        cf=cf,
        catfolder=args.science_db_catalogs,
    )


if __name__ == "__main__":
    main()
