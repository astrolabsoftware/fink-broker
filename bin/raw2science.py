#!/usr/bin/env python
# Copyright 2019-2020 AstroLab Software
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
"""Update the (tmp) science database from the raw database alert data.

Step 1: Connect to the raw database
Step 2: Filter alerts based on instrumental or environmental criteria.
Step 3: Run processors (aka science modules) on alerts to generate added value.
Step 4: Push alert data into the tmp science database (parquet)

See http://cdsxmatch.u-strasbg.fr/ for more information on the SIMBAD catalog.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import argparse
import time
import json

from fink_broker.parser import getargs
from fink_broker.sparkUtils import init_sparksession
from fink_broker.sparkUtils import connect_to_raw_database
from fink_broker.filters import apply_user_defined_filter
from fink_broker.filters import apply_user_defined_processors
from fink_broker.loggingUtils import get_fink_logger, inspect_application

from fink_science.xmatch.processor import cdsxmatch

from fink_science.random_forest_snia.processor import rfscore_sigmoid_full
from fink_science.utilities import concat_col

from fink_science.snn.processor import snn_ia

from fink_science.microlensing.processor import mulens
from fink_science.microlensing.classifier import load_mulens_schema_twobands

qualitycuts = 'fink_broker.filters.qualitycuts'

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(name="raw2science", shuffle_partitions=2)

    # Logger to print useful debug statements
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    df = connect_to_raw_database(
        args.rawdatapath, args.rawdatapath + "/*", latestfirst=False)

    # Apply level one filters
    logger.info(qualitycuts)
    df = apply_user_defined_filter(df, qualitycuts)

    # Retrieve time-series information
    to_expand = [
        'jd', 'fid', 'magpsf', 'sigmapsf',
        'magnr', 'sigmagnr', 'magzpsci', 'isdiffpos'
    ]

    # Append temp columns with historical + current measurements
    prefix = 'c'
    for colname in to_expand:
        df = concat_col(df, colname, prefix=prefix)
    expanded = [prefix + i for i in to_expand]

    # Apply level one processor: cdsxmatch
    logger.info("New processor: cdsxmatch")
    colnames = [
        df['objectId'],
        df['candidate.ra'],
        df['candidate.dec']
    ]
    df = df.withColumn('cdsxmatch', cdsxmatch(*colnames))

    # Apply level one processor: rfscore
    logger.info("New processor: rfscore")

    # Perform the fit + classification.
    # Note we can omit the model_path argument, and in that case the
    # default model `data/models/default-model.obj` will be used.
    rfscore_args = ['cjd', 'cfid', 'cmagpsf', 'csigmapsf']
    df = df.withColumn(
        'rfscore',
        rfscore_sigmoid_full(*rfscore_args)
    )

    # Apply level one processor: rfscore
    logger.info("New processor: supernnova")

    snn_args = ['candid', 'cjd', 'cfid', 'cmagpsf', 'csigmapsf']
    df = df.withColumn('snnscore', snn_ia(*snn_args))

    # Apply level one processor: rfscore
    logger.info("New processor: microlensing")

    # Retrieve schema
    schema = load_mulens_schema_twobands()

    # Create standard UDF
    mulens_udf = F.udf(mulens, schema)

    # Required alert columns - already computed for SN
    mulens_args = [
        'cfid', 'cmagpsf', 'csigmapsf',
        'cmagnr', 'csigmagnr', 'cmagzpsci', 'cisdiffpos']
    df = df.withColumn('mulens', mulens_udf(*mulens_args))

    # Drop temp columns
    df = df.drop(*expanded)

    # re-create partitioning columns.
    # Partitioned data doesn't preserve type information (cast as int...)
    df_partitionedby = df\
        .withColumn("year", F.date_format("timestamp", "yyyy"))\
        .withColumn("month", F.date_format("timestamp", "MM"))\
        .withColumn("day", F.date_format("timestamp", "dd"))

    # Append new rows in the tmp science database
    countquery = df_partitionedby\
        .writeStream\
        .outputMode("append") \
        .format("parquet") \
        .option("checkpointLocation", args.checkpointpath_sci_tmp) \
        .option("path", args.scitmpdatapath)\
        .partitionBy("year", "month", "day") \
        .start()

    # Keep the Streaming running until something or someone ends it!
    if args.exit_after is not None:
        time.sleep(args.exit_after)
        countquery.stop()
        logger.info("Exiting the raw2science service normally...")
    else:
        # Wait for the end of queries
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
