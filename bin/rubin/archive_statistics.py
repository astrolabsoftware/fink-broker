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

    # per band
    perband = df.groupby("diaSource.band").count().collect()
    [out_dic.update({"alerts_{}".format(i["band"]): i["count"]}) for i in perband]

    # Number of objects (removing asteroids)
    out_dic["objects"] = df.select("diaObject.diaObjectId").distinct().count() - 1

    # SSO, first, cataloged
    out_dic["is_sso"] = df.filter(df["pred.is_sso"]).count()
    out_dic["is_first"] = df.filter(df["pred.is_first"]).count()
    out_dic["is_cataloged"] = df.filter(df["pred.is_cataloged"]).count()

    # FIXME: is_candidate?

    # SIMBAD
    simbad = df.groupBy("pred.main_label_crossmatch").count().collect()
    [
        out_dic.update({"SIMBAD_{}".format(i["main_label_crossmatch"]): i["count"]})
        for i in simbad
    ]

    # TNS
    tns = df.groupBy("xm.tns_type").count().collect()
    [out_dic.update({"TNS_{}".format(i["tns_type"]): i["count"]}) for i in tns]

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
