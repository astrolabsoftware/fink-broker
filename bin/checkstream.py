#!/usr/bin/env python
# Copyright 2019-2021 AstroLab Software
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
"""Monitor Kafka stream received by Spark"""

import argparse
import time

from fink_broker.parser import getargs
from fink_broker.sparkUtils import init_sparksession, connect_to_kafka
from fink_broker.monitoring import monitor_progress_webui
from fink_broker.loggingUtils import get_fink_logger, inspect_application


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Initialise Spark session
    spark = init_sparksession(name="checkstream", shuffle_partitions=2)

    # The level here should be controlled by an argument.
    logger = get_fink_logger(spark.sparkContext.appName, args.log_level)

    # debug statements
    inspect_application(logger)

    # Create a streaming dataframe pointing to a Kafka stream
    df = connect_to_kafka(
        servers=args.servers,
        topic=args.topic,
        startingoffsets=args.startingoffsets_stream,
        failondataloss=False,
    )

    # Trigger the streaming computation,
    # by defining the sink (memory here) and starting it
    countquery = (
        df.writeStream.queryName("qraw").format("console").outputMode("update").start()
    )

    # Monitor the progress of the stream, and save data for the webUI
    colnames = ["inputRowsPerSecond", "processedRowsPerSecond", "timestamp"]
    monitor_progress_webui(
        countquery, 2, colnames, args.finkwebpath, "live_raw.csv", "live"
    )

    # Keep the Streaming running until something or someone ends it!
    if args.exit_after is not None:
        time.sleep(args.exit_after)
        countquery.stop()
        logger.info("Exiting the checkstream service normally...")
    else:
        countquery.awaitTermination()


if __name__ == "__main__":
    main()
