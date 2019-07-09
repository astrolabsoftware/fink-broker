#!/usr/bin/env python
# Copyright 2019 AstroLab Software
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
"""Update the science database from the raw database alert data.

Step 1: Connect to the raw database
Step 2: Filter alerts based on instrumental or environmental criteria.
Step 3: Classify alerts using the xMatch service at CDS.
Step 4: Push alert data into the science database (TBD)

See http://cdsxmatch.u-strasbg.fr/ for more information on the SIMBAD catalog.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

import argparse
import time
import json

from fink_broker.parser import getargs
from fink_broker.sparkUtils import init_sparksession
from fink_broker.sparkUtils import connect_to_raw_database
from fink_broker.sparkUtils import write_to_csv
from fink_broker.hbaseUtils import flattenstruct, explodearrayofstruct
from fink_broker.hbaseUtils import construct_hbase_catalog_from_flatten_schema
from fink_broker.filters import apply_user_defined_filters
from fink_broker.filters import apply_user_defined_processors
from fink_broker.loggingUtils import get_fink_logger, inspect_application

from userfilters.levelone import filter_levelone_names
from userfilters.levelone import processor_levelone_names

from pyspark.sql.functions import lit

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(name="raw2science", shuffle_partitions=2)

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    # FIXME!
    if "travis" in args.science_db_name:
        latesfirst = False
    else:
        latesfirst = True

    df = connect_to_raw_database(
        args.rawdatapath, args.rawdatapath + "/*", latesfirst)

    # Apply level one filters
    df = apply_user_defined_filters(df, filter_levelone_names)

    # Apply level one processors
    df = apply_user_defined_processors(df, processor_levelone_names)

    # Select alert data + timestamp + added value from processors
    new_colnames = ["decoded.*", "cast(timestamp as string) as timestamp"]
    for i in processor_levelone_names:
        new_colnames.append(i)

    df = df.selectExpr(new_colnames)

    df_hbase = flattenstruct(df, "candidate")
    df_hbase = flattenstruct(df_hbase, "cutoutScience")
    df_hbase = flattenstruct(df_hbase, "cutoutTemplate")
    df_hbase = flattenstruct(df_hbase, "cutoutDifference")
    df_hbase = explodearrayofstruct(df_hbase, "prv_candidates")

    # Create a status column for distribution
    df_hbase = df_hbase.withColumn("status", lit("dbUpdate"))

    # Save the catalog on disk for later usage
    catalog = construct_hbase_catalog_from_flatten_schema(
        df_hbase.schema, args.science_db_name, "objectId")

    science_db_catalog = args.science_db_catalog
    with open(science_db_catalog, 'w') as json_file:
        json.dump(catalog, json_file)

    def write_to_hbase_and_monitor(
            df: DataFrame, epochid: int, hbcatalog: str):
        """Write data into HBase.

        The purpose of this function is to write data to HBase using
        Structured Streaming tools such as foreachBatch.

        Parameters
        ----------
        df : DataFrame
            Input micro-batch DataFrame.
        epochid : int
            ID of the micro-batch
        hbcatalog : str
            HBase catalog describing the data

        """
        # If the table does not exist, one needs to specify
        # the number of zones to use (must be greater than 3).
        # TODO: remove this harcoded parameter.
        df.write\
            .options(catalog=hbcatalog, newtable=5)\
            .format("org.apache.spark.sql.execution.datasources.hbase")\
            .save()

    # Query to push data into HBase
    countquery = df_hbase\
        .writeStream\
        .outputMode("append")\
        .option("checkpointLocation", args.checkpointpath_sci)\
        .foreachBatch(lambda x, y: write_to_hbase_and_monitor(x, y, catalog))\
        .start()

    # Query to group objects by type according to SIMBAD
    # Do it every 30 seconds
    groupedquery_started = False
    if "cross_match_alerts_per_batch" in processor_levelone_names:
        df_group = df.groupBy("cross_match_alerts_per_batch").count()
        groupquery = df_group\
            .writeStream\
            .outputMode("complete") \
            .foreachBatch(write_to_csv)\
            .trigger(processingTime='30 seconds'.format(args.tinterval))\
            .start()
        groupedquery_started = True

    # Keep the Streaming running until something or someone ends it!
    if args.exit_after is not None:
        time.sleep(args.exit_after)
        countquery.stop()
        if groupedquery_started:
            groupquery.stop()
        logger.info("Exiting the raw2science service normally...")
    else:
        # Wait for the end of queries
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
