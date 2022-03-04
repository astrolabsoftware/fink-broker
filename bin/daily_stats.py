#!/usr/bin/env python
# Copyright 2021 AstroLab Software
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
"""Compute statistics for a given observing night
"""
import argparse

import numpy as np
import pandas as pd

from fink_broker.sparkUtils import init_sparksession

from fink_broker.hbaseUtils import construct_hbase_catalog_from_flatten_schema
from fink_broker.hbaseUtils import construct_schema_row

from fink_broker.parser import getargs
from fink_broker.loggingUtils import get_fink_logger, inspect_application

from fink_broker import __version__ as fbvsn
from fink_science import __version__ as fsvsn

from fink_filters.classification import extract_fink_classification


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(
        name="statistics_{}".format(args.night),
        shuffle_partitions=2
    )

    # Logger to print useful debug statements
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    year = args.night[:4]
    month = args.night[4:6]
    day = args.night[6:8]

    print('Statistics for {}/{}/{}'.format(year, month, day))

    input_raw = '{}/raw/year={}/month={}/day={}'.format(
        args.agg_data_prefix, year, month, day)
    input_science = '{}/science/year={}/month={}/day={}'.format(
        args.agg_data_prefix, year, month, day)

    df_raw = spark.read.format('parquet').load(input_raw)
    df_sci = spark.read.format('parquet').load(input_science)

    df_sci = df_sci.cache()

    # Number of alerts
    n_raw_alert = df_raw.count()
    n_sci_alert = df_sci.count()

    out_dic = {}
    out_dic['raw'] = n_raw_alert
    out_dic['sci'] = n_sci_alert

    # matches with SIMBAD
    n_simbad = df_sci.select('cdsxmatch')\
        .filter(df_sci['cdsxmatch'] != 'Unknown')\
        .count()

    out_dic['simbad_tot'] = n_simbad

    # Alerts with a close-by candidate host-galaxy
    list_simbad_galaxies = [
        "galaxy",
        "Galaxy",
        "EmG",
        "Seyfert",
        "Seyfert_1",
        "Seyfert_2",
        "BlueCompG",
        "StarburstG",
        "LSB_G",
        "HII_G",
        "High_z_G",
        "GinPair",
        "GinGroup",
        "BClG",
        "GinCl",
        "PartofG",
    ]

    n_simbad_gal = df_sci.select('cdsxmatch')\
        .filter(df_sci['cdsxmatch'].isin(list_simbad_galaxies))\
        .count()

    out_dic['simbad_gal'] = n_simbad_gal

    df_class = df_sci.withColumn(
        'class',
        extract_fink_classification(
            df_sci['cdsxmatch'],
            df_sci['roid'],
            df_sci['mulens'],
            df_sci['snn_snia_vs_nonia'],
            df_sci['snn_sn_vs_all'],
            df_sci['rf_snia_vs_nonia'],
            df_sci['candidate.ndethist'],
            df_sci['candidate.drb'],
            df_sci['candidate.classtar'],
            df_sci['candidate.jd'],
            df_sci['candidate.jdstarthist'],
            df_sci['rf_kn_vs_nonkn'],
            df_sci['tracklet']
        )
    )

    out_class = df_class.groupBy('class').count().collect()
    out_class_ = [o.asDict() for o in out_class]
    out_class_ = [list(o.values()) for o in out_class_]
    for kv in out_class_:
        out_dic[kv[0]] = kv[1]

    # Number of fields
    n_field = df_sci.select('candidate.field').distinct().count()

    out_dic['fields'] = n_field

    # number of measurements per band
    n_g = df_sci.select('candidate.fid').filter('fid == 1').count()
    n_r = df_sci.select('candidate.fid').filter('fid == 2').count()

    out_dic['n_g'] = n_g
    out_dic['n_r'] = n_r

    # Number of exposures
    n_exp = df_sci.select('candidate.jd').distinct().count()

    out_dic['exposures'] = n_exp

    out_dic['night'] = 'ztf_{}'.format(args.night)

    # make a Spark DataFrame
    pdf = pd.DataFrame([out_dic])
    df_hbase = spark.createDataFrame(pdf)

    # rowkey is the night YYYYMMDD
    index_row_key_name = 'night'

    # Columns to use
    cols_basic = [
        'raw',
        'sci',
        'night',
        'n_g',
        'n_r',
        'exposures',
        'fields'
    ]

    cols_class_ = np.transpose(out_class_)[0]
    cols_class = np.concatenate((cols_class_, ['simbad_tot', 'simbad_gal']))

    # column families
    cf = {i: 'basic' for i in df_hbase.select(*cols_basic).columns}
    cf.update({i: 'class' for i in df_hbase.select(*cols_class).columns})

    # construct the time catalog
    hbcatalog_index = construct_hbase_catalog_from_flatten_schema(
        df_hbase.schema,
        'statistics_class',
        rowkeyname=index_row_key_name,
        cf=cf
    )

    # Push index table
    df_hbase.write\
        .options(catalog=hbcatalog_index, newtable=50)\
        .format("org.apache.hadoop.hbase.spark")\
        .option("hbase.spark.use.hbasecontext", False)\
        .save()

    # Construct the schema row - inplace replacement
    schema_row_key_name = 'schema_version'
    df_hbase = df_hbase.withColumnRenamed(
        index_row_key_name,
        schema_row_key_name
    )

    df_hbase_schema = construct_schema_row(
        df_hbase,
        rowkeyname=schema_row_key_name,
        version='schema_{}_{}'.format(fbvsn, fsvsn))

    # construct the hbase catalog for the schema
    hbcatalog_index_schema = construct_hbase_catalog_from_flatten_schema(
        df_hbase_schema.schema,
        'statistics_class',
        rowkeyname=schema_row_key_name,
        cf=cf)

    # Push the data using the shc connector
    df_hbase_schema.write\
        .options(catalog=hbcatalog_index_schema, newtable=50)\
        .format("org.apache.hadoop.hbase.spark")\
        .option("hbase.spark.use.hbasecontext", False)\
        .save()


if __name__ == "__main__":
    main()
