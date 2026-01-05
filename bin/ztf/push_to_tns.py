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
"""Push early SN candidates to TNS"""

import argparse
import requests
import os

from fink_broker.common.parser import getargs
from fink_broker.common.spark_utils import init_sparksession, load_parquet_files
from fink_broker.common.logging_utils import get_fink_logger, inspect_application

from fink_filters.ztf.classification import extract_fink_classification

from fink_tns.utils import read_past_ids, retrieve_groupid
from fink_tns.report import extract_discovery_photometry, build_report
from fink_tns.report import save_logs_and_return_json_report, send_json_report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(
        name="TNS_report_{}".format(args.night), shuffle_partitions=2
    )

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    # Connect to the aggregated science database
    path = "{}/science/year={}/month={}/day={}".format(
        args.agg_data_prefix, args.night[:4], args.night[4:6], args.night[6:8]
    )
    df = load_parquet_files(path)

    with open("{}/tns_marker.txt".format(args.tns_folder)) as f:
        tns_marker = f.read().replace("\n", "")

    url_tns_api = "https://www.wis-tns.org/api"
    with open("{}/tns_api.key".format(args.tns_folder)) as f:
        # remove line break...
        key = f.read().replace("\n", "")

    cols = [
        "cdsxmatch",
        "roid",
        "mulens",
        "snn_snia_vs_nonia",
        "snn_sn_vs_all",
        "rf_snia_vs_nonia",
        "candidate.ndethist",
        "candidate.drb",
        "candidate.classtar",
        "candidate.jd",
        "candidate.jdstarthist",
        "rf_kn_vs_nonkn",
        "tracklet",
    ]
    df = df.withColumn("class", extract_fink_classification(*cols))

    pdf = (
        df
        .filter(df["class"] == "Early SN Ia candidate")
        .filter(df["candidate.ndethist"] <= 20)
        .toPandas()
    )

    pdf_unique = pdf.groupby("objectId")[pdf.columns].min()
    print("{} new alerts".format(len(pdf)))
    print("{} new sources".format(len(pdf_unique)))
    pdf = pdf_unique

    ids = []
    report = {"at_report": {}}
    check_tns = False
    for index, row in enumerate(pdf.iterrows()):
        alert = row[1]
        past_ids = read_past_ids(args.tns_folder)
        if alert["objectId"] in past_ids.to_numpy():
            print("{} already sent!".format(alert["objectId"]))
            continue
        if check_tns:
            groupid = retrieve_groupid(key, tns_marker, alert["objectId"])
            if groupid > 0:
                print("{} already reported by {}".format(alert["objectId"], groupid))
            else:
                print("New report for object {}".format(alert["objectId"]))
        photometry, non_detection = extract_discovery_photometry(alert)
        report["at_report"]["{}".format(index)] = build_report(
            alert, photometry, non_detection
        )
        ids.append(alert["objectId"])
    print("new objects: ", ids)

    if len(ids) != 0:
        json_report = save_logs_and_return_json_report(
            name="{}{}{}".format(args.night[:4], args.night[4:6], args.night[6:8]),
            folder=args.tns_folder,
            ids=ids,
            report=report,
        )
        r = send_json_report(key, url_tns_api, json_report, tns_marker)
        print(r.json())

        # post to slack
        slacktxt = " \n ".join(["https://fink-portal.org/{}".format(i) for i in ids])
        slacktxt = "{} \n ".format(args.night) + slacktxt
        r = requests.post(
            os.environ["TNSWEBHOOK"],
            json={"text": slacktxt, "username": "VirtualData"},
            headers={"Content-Type": "application/json"},
        )
        print(r.status_code)
    else:
        slacktxt = "{} \n No new sources".format(args.night)
        r = requests.post(
            os.environ["TNSWEBHOOK"],
            json={"text": slacktxt, "username": "VirtualData"},
            headers={"Content-Type": "application/json"},
        )


if __name__ == "__main__":
    main()
