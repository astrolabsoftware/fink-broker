#!/usr/bin/env python
# Copyright 2020-2021 AstroLab Software
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
import pyspark.sql.functions as F
from pyspark.sql.functions import lit, concat_ws, col
from pyspark.sql.functions import arrays_zip, explode
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StringType

import argparse
import os
import numpy as np
import pandas as pd

from fink_broker import __version__ as fbvsn
from fink_broker.parser import getargs
from fink_broker.sparkUtils import init_sparksession, load_parquet_files

from fink_broker.hbaseUtils import construct_hbase_catalog_from_flatten_schema
from fink_broker.hbaseUtils import load_science_portal_column_names
from fink_broker.hbaseUtils import assign_column_family_names
from fink_broker.hbaseUtils import attach_rowkey
from fink_broker.hbaseUtils import construct_schema_row
from fink_broker.science import extract_fink_classification
from fink_broker.science import ang2pix, ang2pix_array

from fink_tns.utils import download_catalog

from astropy.coordinates import SkyCoord
from astropy import units as u

from fink_broker.loggingUtils import get_fink_logger, inspect_application

from fink_science import __version__ as fsvsn


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(
        name="index_archival_{}_{}".format(args.index_table, args.night),
        shuffle_partitions=2
    )

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    # Push data monthly
    path = 'ztf_alerts/science/year={}/month={}/day={}'.format(
        args.night[:4],
        args.night[4:6],
        args.night[6:8]
    )
    df = load_parquet_files(path)

    # to account for schema migration
    if 'knscore' not in df.columns:
        df = df.withColumn('knscore', lit(-1.0))

    # Drop partitioning columns
    df = df.drop('year').drop('month').drop('day')

    # Load column names to use in the science portal
    cols_i, cols_d, cols_b = load_science_portal_column_names()

    # Assign each column to a specific column family
    cf = assign_column_family_names(df, cols_i, cols_d, cols_b)

    # Restrict the input DataFrame to the subset of wanted columns.
    if 'upper' in args.index_table:
        df = df.select(
            'objectId',
            'prv_candidates.jd',
            'prv_candidates.fid',
            'prv_candidates.magpsf',
            'prv_candidates.sigmapsf',
            'prv_candidates.diffmaglim'
        )
    else:
        df = df.select(cols_i + cols_d + cols_b)

    # Create and attach the rowkey
    df, _ = attach_rowkey(df)

    # construct the index view
    index_row_key_name = args.index_table
    columns = index_row_key_name.split('_')
    names = [col(i) for i in columns]
    index_name = '.' + columns[0]

    common_cols = [
        'objectId', 'candid', 'publisher', 'rcid', 'chipsf', 'distnr',
        'ra', 'dec', 'jd', 'fid', 'nid', 'field', 'xpos', 'ypos', 'rb',
        'ssdistnr', 'ssmagnr', 'ssnamenr', 'jdstarthist', 'jdendhist', 'tooflag',
        'sgscore1', 'distpsnr1', 'neargaia', 'maggaia', 'nmtchps', 'diffmaglim',
        'magpsf', 'sigmapsf', 'magnr', 'sigmagnr', 'magzpsci', 'isdiffpos',
        'cdsxmatch',
        'roid',
        'mulens_class_1', 'mulens_class_2',
        'snn_snia_vs_nonia', 'snn_sn_vs_all', 'rfscore',
        'classtar', 'drb', 'ndethist', 'knscore'
    ]

    if columns[0].startswith('pixel'):
        nside = int(columns[0].split('pixel')[1])

        df_index = df.withColumn(
            'pixel',
            ang2pix(
                df['ra'],
                df['dec'],
                lit(nside)
            )
        ).select(
            [
                concat_ws('_', *names).alias(index_row_key_name)
            ] + ['objectId']
        )
    elif columns[0] == 'class':
        df_index = df.withColumn(
            'class',
            extract_fink_classification(
                df['cdsxmatch'],
                df['roid'],
                df['mulens_class_1'],
                df['mulens_class_2'],
                df['snn_snia_vs_nonia'],
                df['snn_sn_vs_all'],
                df['rfscore'],
                df['ndethist'],
                df['drb'],
                df['classtar'],
                df['jd'],
                df['jdstarthist'],
                df['knscore']
            )
        ).select(
            [
                concat_ws('_', *names).alias(index_row_key_name)
            ] + common_cols
        )
    elif columns[0] == 'ssnamenr':
        # TODO: Computation of SSO flags was bugged.
        # Ideally, we would have to filter on the `roid==3` field, but
        # there was a bug in its computation (see https://github.com/astrolabsoftware/fink-science/issues/85)
        # Hence, as long as we the data is not recomputed, we use this condition
        # to flag known SSO (which is actually used since 02/2021).
        df_index = df\
            .filter(df['ssnamenr'] != 'null')\
            .filter(df['ssdistnr'] >= 0)\
            .filter(df['ssdistnr'] < 5)\
            .filter((F.abs(df['distpsnr1']) - df['ssdistnr']) > 0.0)\
            .filter(df['ndethist'] <= 2)\
            .select(
                [
                    concat_ws('_', *names).alias(index_row_key_name)
                ] + common_cols
            )
    elif columns[0] == 'upper':
        # This case is the same as the main table
        # but we keep only upper limit measurements.
        index_row_key_name = 'objectId_jd'
        # explode
        df_ex = df.withColumn(
            "tmp",
            arrays_zip("magpsf", "sigmapsf", "diffmaglim", "jd", "fid")
        ).withColumn("tmp", explode("tmp")).select(
            concat_ws('_', 'objectId', 'tmp.jd').alias(index_row_key_name),
            "objectId",
            col("tmp.jd"),
            col("tmp.fid"),
            col("tmp.magpsf"),
            col("tmp.sigmapsf"),
            col("tmp.diffmaglim")
        )

        # take only upper limits
        df_index = df_ex.filter(~df_ex['magpsf'].isNotNull())
        # drop NaN columns
        df_index = df_index.drop(*['magpsf', 'sigmapsf'])
    elif columns[0] == 'uppervalid':
        # This case is the same as the main table
        # but we keep only upper limit measurements.
        index_row_key_name = 'objectId_jd'
        # explode
        df_ex = df.withColumn(
            "tmp",
            arrays_zip("magpsf", "sigmapsf", "diffmaglim", "jd", "fid")
        ).withColumn("tmp", explode("tmp")).select(
            concat_ws('_', 'objectId', 'tmp.jd').alias(index_row_key_name),
            "objectId",
            col("tmp.jd"),
            col("tmp.fid"),
            col("tmp.magpsf"),
            col("tmp.sigmapsf"),
            col("tmp.diffmaglim")
        )

        # take only valid measurements from the history
        df_index = df_ex.filter(df_ex['magpsf'].isNotNull())
    elif columns[0] == 'tns':
        with open('{}/tns_marker.txt'.format(args.tns_folder)) as f:
            tns_marker = f.read().replace('\n', '')

        pdf_tns = download_catalog(os.environ['TNS_API_KEY'], tns_marker)

        # Filter TNS confirmed data
        f1 = ~pdf_tns['type'].isna()
        pdf_tns_filt = pdf_tns[f1]

        pdf_tns_filt_b = spark.sparkContext.broadcast(pdf_tns_filt)

        @pandas_udf(StringType(), PandasUDFType.SCALAR)
        def crossmatch_with_tns(objectid, ra, dec):
            # TNS
            pdf = pdf_tns_filt_b.value
            ra2, dec2, type2 = pdf['ra'], pdf['declination'], pdf['type']

            # create catalogs
            catalog_ztf = SkyCoord(
                ra=np.array(ra, dtype=np.float) * u.degree,
                dec=np.array(dec, dtype=np.float) * u.degree
            )
            catalog_tns = SkyCoord(
                ra=np.array(ra2, dtype=np.float) * u.degree,
                dec=np.array(dec2, dtype=np.float) * u.degree
            )

            # cross-match
            idx, d2d, d3d = catalog_tns.match_to_catalog_sky(catalog_ztf)

            sub_pdf = pd.DataFrame({
                'objectId': objectid.values[idx],
                'ra': ra.values[idx],
                'dec': dec.values[idx],
            })

            # cross-match
            idx2, d2d2, d3d2 = catalog_ztf.match_to_catalog_sky(catalog_tns)

            # set separation length
            sep_constraint2 = d2d2.degree < 1.5 / 3600

            sub_pdf['TNS'] = [''] * len(sub_pdf)
            sub_pdf['TNS'][idx2[sep_constraint2]] = type2.values[idx2[sep_constraint2]]

            to_return = objectid.apply(
                lambda x: '' if x not in sub_pdf['objectId'].values
                else sub_pdf['TNS'][sub_pdf['objectId'] == x].values[0]
            )

            return to_return

        df = df.withColumn(
            'tns',
            crossmatch_with_tns(
                df['objectId'],
                df['ra'],
                df['dec']
            )
        ).select(
            [
                concat_ws('_', *names).alias(index_row_key_name)
            ] + common_cols + ['tns']
        ).cache()
        df_index = df.filter(df['tns'] != '').drop('tns')
        # trigger the cache - not the cache might be a killer for LSST...
        n = df_index.count()
        print('TNS objects: {}'.format(n))
    else:
        df_index = df.select(
            [
                concat_ws('_', *names).alias(index_row_key_name)
            ] + common_cols
        )

    # construct the time catalog
    hbcatalog_index = construct_hbase_catalog_from_flatten_schema(
        df_index.schema,
        args.science_db_name + index_name,
        rowkeyname=index_row_key_name,
        cf=cf
    )

    # Push index table
    df_index.write\
        .options(catalog=hbcatalog_index, newtable=50)\
        .format("org.apache.spark.sql.execution.datasources.hbase")\
        .save()

    # Construct the schema row - inplace replacement
    schema_row_key_name = 'schema_version'
    df_index = df_index.withColumnRenamed(
        index_row_key_name,
        schema_row_key_name
    )

    df_index_schema = construct_schema_row(
        df_index,
        rowkeyname=schema_row_key_name,
        version='schema_{}_{}'.format(fbvsn, fsvsn))

    # construct the hbase catalog for the schema
    hbcatalog_index_schema = construct_hbase_catalog_from_flatten_schema(
        df_index_schema.schema,
        args.science_db_name + index_name,
        rowkeyname=schema_row_key_name,
        cf=cf)

    # Push the data using the shc connector
    df_index_schema.write\
        .options(catalog=hbcatalog_index_schema, newtable=50)\
        .format("org.apache.spark.sql.execution.datasources.hbase")\
        .save()


if __name__ == "__main__":
    main()
