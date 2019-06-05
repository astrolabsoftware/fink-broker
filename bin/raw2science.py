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
from fink_broker.filters import keep_alert_based_on
from fink_broker.classification import cross_match_alerts_per_batch
from fink_broker.hbaseUtils import flattenstruct, explodearrayofstruct
from fink_broker.hbaseUtils import construct_hbase_catalog_from_flatten_schema

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Grab the running Spark Session,
    # otherwise create it.
    spark = init_sparksession(
        name="buildSciDB", shuffle_partitions=2, log_level="ERROR")

    # FIXME!
    if "travis" in args.science_db_name:
        latesfirst = False
    else:
        latesfirst = True

    df = connect_to_raw_database(
        args.rawdatapath, args.rawdatapath + "/*", latesfirst)

    # Apply filters and keep only good alerts
    df_filt = df.withColumn(
        "toKeep",
        keep_alert_based_on(
            col("decoded.candidate.nbad"),
            col("decoded.candidate.rb"),
            col("decoded.candidate.magdiff")
        )
    ).filter("toKeep == true")

    # for good alerts, perform a cross-match with SIMBAD,
    # and return the types of the objects (Star, AGN, Unknown, etc.)
    df_type = df_filt.withColumn(
        "simbadType",
        cross_match_alerts_per_batch(
            col("decoded.objectId"),
            col("decoded.candidate.ra"),
            col("decoded.candidate.dec")
        )
    ).selectExpr(
        "decoded.*", "cast(timestamp as string) as timestamp", "simbadType")

    df_hbase = flattenstruct(df_type, "candidate")
    df_hbase = flattenstruct(df_hbase, "cutoutScience")
    df_hbase = flattenstruct(df_hbase, "cutoutTemplate")
    df_hbase = flattenstruct(df_hbase, "cutoutDifference")
    df_hbase = explodearrayofstruct(df_hbase, "prv_candidates")

    catalog = construct_hbase_catalog_from_flatten_schema(
        df_hbase.schema, args.science_db_name, "objectId")

<<<<<<< HEAD
    science_db_catalog = args.science_db_catalog
    with open(science_db_catalog, 'w') as json_file:
=======
    # Save the catalog on disk for later usage
    with open('catalog.json', 'w') as json_file:
>>>>>>> master
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
    df_group = df_type.groupBy("simbadType").count()
    groupquery = df_group\
        .writeStream\
        .outputMode("complete") \
        .foreachBatch(write_to_csv)\
        .trigger(processingTime='30 seconds'.format(args.tinterval))\
        .start()

    # Keep the Streaming running until something or someone ends it!
    if args.exit_after is not None:
        time.sleep(args.exit_after)
        countquery.stop()
        groupquery.stop()
        print("Exiting the raw2science service normally...")
    else:
        # Wait for the end of queries
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
