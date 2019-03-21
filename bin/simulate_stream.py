#!/usr/bin/env python
# Copyright 2018 AstroLab Software
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
"""Simulate batches of alerts coming from ZTF.
"""
import argparse
import os
import glob
import time
import asyncio
from fink_broker import alertProducer
from fink_broker import avroUtils

from fink_broker.parser import getargs

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    args = getargs(parser)

    # Configure producer connection to Kafka broker
    conf = {'bootstrap.servers': args.servers}
    streamProducer = alertProducer.AlertProducer(
        args.topic, schema_files=None, **conf)

    # Scan for avro files
    root = args.datapath

    # Grab data stored on disk
    files = glob.glob(os.path.join(root, "*.avro"))

    # Replicate alerts if necessary
    if len(files) < args.poolsize:
        nreplication = args.poolsize // len(files) + 1
        files = files * nreplication

    def send_visit(fn):
        """ Send alert for publication in Kafka

        Parameters
        ----------
        fn: str
            Filename containing the alert (avro file)
        """
        print('Alert sent - time: ', time.time())
        # Load alert contents
        with open(fn, mode='rb') as file_data:
            # Read the data
            data = avroUtils.readschemadata(file_data)

            # Read the Schema
            schema = data.schema

            # Send the alerts
            alert_count = 0
            for record in data:
                if alert_count < 10000:
                    streamProducer.send(
                        record, alert_schema=schema, encode=True)
                    alert_count += 1
                else:
                    break
        streamProducer.flush()

    loop = asyncio.get_event_loop()
    asyncio.ensure_future(
        alertProducer.schedule_delays(
            loop,
            send_visit,
            files[:args.poolsize],
            interval=args.tinterval_kafka))
    loop.run_forever()
    loop.close()


if __name__ == "__main__":
    main()
