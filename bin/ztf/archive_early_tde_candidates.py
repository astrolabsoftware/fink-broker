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
"""Run the early TDE candidates filter, and push data to Slack/Telegram."""

import os
import argparse

from fink_broker.common.parser import getargs
from fink_broker.common.spark_utils import init_sparksession, load_parquet_files
from fink_broker.common.logging_utils import get_fink_logger, inspect_application

from fink_filters.ztf.filter_early_tde_candidates.filter import early_tde_candidates


def main():
    """Extract probabilities from the early TDE candidates filter, and send to TG/Slack"""
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(
        name="early_tde_candidates_{}".format(args.night), shuffle_partitions=2
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

    _ = early_tde_candidates(
        df,
        send_to_tg="EARLY_TDE_TG_TOKEN" in os.environ,
        send_to_slack="EARLY_TDE_SLACK_TOKEN" in os.environ,
    )


if __name__ == "__main__":
    main()
