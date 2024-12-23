#!/usr/bin/env python
# Copyright 2023-2024 AstroLab Software
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
"""Send anomaly detections via Slack & Telegram"""

import argparse

from fink_broker.parser import getargs
from fink_broker.spark_utils import init_sparksession, load_parquet_files

from fink_filters.filter_anomaly_notification.filter import anomaly_notification_
from fink_science.anomaly_detection.processor import ANOMALY_MODELS
from fink_broker.logging_utils import get_fink_logger, inspect_application

from fink_broker.hbase_utils import push_full_df_to_hbase


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(
        name="anomaly_archival_{}".format(args.night), shuffle_partitions=2
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

    # Send anomalies
    df_proc = df.select(
        "objectId",
        "candid",
        "candidate.ra",
        "candidate.dec",
        "candidate.rb",
        "anomaly_score",
        "timestamp",
    )

    # All-sky anomalies
    pdf = anomaly_notification_(
        df_proc,
        threshold=10,
        send_to_tg=True,
        channel_id="@ZTF_anomaly_bot",
        send_to_slack=True,
        channel_name="bot_anomaly",
    )

    # Area-restricted anomalies
    # We do not store candidates
    anomaly_notification_(
        df_proc,
        threshold=5,
        send_to_tg=True,
        channel_id="@anomaly_spec",
        send_to_slack=True,
        channel_name="bot_anomaly_area",
        cut_coords=True,
    )

    for model in ANOMALY_MODELS:
        df_proc = df.select(
            "objectId",
            "candidate.ra",
            "candidate.dec",
            "candidate.rb",
            f"anomaly_score{model}",
            "timestamp",
        )
        anomaly_notification_(
            df_proc,
            send_to_tg=False,
            send_to_slack=False,
            send_to_anomaly_base=True,
            model=model,
        )

    # Keep only candidates of interest for all sky anomalies
    oids = [int(i) for i in pdf["candid"].to_numpy()]
    df_hbase = df.filter(df["candid"].isin(list(oids)))

    # Row key
    row_key_name = "jd_objectId"

    # push data to HBase
    push_full_df_to_hbase(
        df_hbase,
        row_key_name=row_key_name,
        table_name=args.science_db_name + ".anomaly",
        catalog_name=args.science_db_catalogs,
    )


if __name__ == "__main__":
    main()
