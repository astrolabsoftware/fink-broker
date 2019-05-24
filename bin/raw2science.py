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
from pyspark.sql.functions import col

import argparse
import time
import json

from fink_broker.parser import getargs
from fink_broker.sparkUtils import init_sparksession
from fink_broker.sparkUtils import connect_to_raw_database
from fink_broker.filters import keep_alert_based_on
from fink_broker.classification import cross_match_alerts_per_batch
from fink_broker.hbaseUtils import flattenstruct, explodearrayofstruct
from fink_broker.hbaseUtils import construct_hbase_catalog_from_flatten_schema

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Grab the running Spark Session,
    # otherwise create it.
    init_sparksession(
        name="buildSciDB", shuffle_partitions=2, log_level="ERROR")

    df = connect_to_raw_database(
        args.rawdatapath, args.rawdatapath + "/*", True)

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

    with open('catalog.json', 'w') as json_file:
        json.dump(catalog, json_file)

    # Push alert to the science database
    options = {
        "hbase.catalog": catalog,
        "checkpointLocation": args.checkpointpath_sci
    }

    # If the table does not exist, one needs to specify
    # the number of zones to use (must be greater than 3).
    if "travis" in args.science_db_name:
        options["hbase.newTable"] = 5

    countquery = df_hbase\
        .writeStream\
        .outputMode("append") \
        .format("HBase.HBaseStreamSinkProvider") \
        .options(**options).start()

    # Keep the Streaming running until something or someone ends it!
    if args.exit_after is not None:
        time.sleep(args.exit_after)
        countquery.stop()
        print("Exiting the archiving service normally...")
    else:
        countquery.awaitTermination()


if __name__ == "__main__":
    main()
