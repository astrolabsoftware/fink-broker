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
from fink_broker.tester import regular_unit_tests

def getargs(parser):
    """ Parse command line arguments for fink services

    Parameters
    ----------
    parser: argparse.ArgumentParser
        Empty parser

    Returns
    ----------
    args: argparse.Namespace
        Object containing CLI arguments parsed

    Examples
    ----------
    >>> import argparse
    >>> parser = argparse.ArgumentParser(description=__doc__)
    >>> args = getargs(parser)
    >>> print(type(args))
    <class 'argparse.Namespace'>
    """
    parser.add_argument(
        '-servers', type=str, default='',
        help="""
        Hostname or IP and port of Kafka broker producing stream.
        [KAFKA_IPPORT/KAFKA_IPPORT_SIM]
        """)
    parser.add_argument(
        '-topic', type=str, default='',
        help="""
        Name of Kafka topic stream to read from.
        [KAFKA_TOPIC/KAFKA_TOPIC_SIM]
        """)
    parser.add_argument(
        '-schema', type=str, default='',
        help="""
        Schema to decode the alert. Should be avro file.
        [FINK_ALERT_SCHEMA]""")
    parser.add_argument(
        '-startingoffsets', type=str, default='',
        help="""From which offset you want to start pulling data.
        [KAFKA_STARTING_OFFSET]
        """)
    parser.add_argument(
        '-outputpath', type=str, default='',
        help="""
        Directory on disk for saving live data.
        [FINK_ALERT_PATH]
        """)
    parser.add_argument(
        '-checkpointpath', type=str, default='',
        help="""
        For some output sinks where the end-to-end fault-tolerance
        can be guaranteed, specify the location where the system will
        write all the checkpoint information. This should be a directory
        in an HDFS-compatible fault-tolerant file system.
        See conf/fink.conf & https://spark.apache.org/docs/latest/
        structured-streaming-programming-guide.html#starting-streaming-queries
        [FINK_ALERT_CHECKPOINT]
        """)
    parser.add_argument(
        '-finkwebpath', type=str, default='',
        help="""
        Folder to store UI data for display.
        [FINK_UI_PATH]
        """)
    parser.add_argument(
        '-tinterval', type=int, default=0,
        help="""
        Time interval between two monitoring. In seconds.
        [FINK_TRIGGER_UPDATE]
        """)
    parser.add_argument(
        '-tinterval_kafka', type=float, default=0.0,
        help="""
        Time interval between two messages are published. In seconds.
        [TIME_INTERVAL]
        """)
    parser.add_argument(
        '-exit_after', type=int, default=None,
        help="""
        Stop the service after `exit_after` seconds.
        This primarily for use on Travis, to stop service after some time.
        Use that with `fink start service --exit_after <time>`. Default is None.
        """)
    parser.add_argument(
        '-datapath', type=str, default='',
        help="""
        Folder containing alerts to be published by Kafka.
        [FINK_DATA_SIM]
        """)
    parser.add_argument(
        '-poolsize', type=int, default=5,
        help="""
        Maximum number of alerts to send. If the poolsize is
        bigger than the number of alerts in `datapath`, then we replicate
        the alerts. Default is 5.
        [POOLSIZE]
        """)
    args = parser.parse_args(None)
    return args


if __name__ == "__main__":
    """ Execute the test suite """

    # Run the regular test suite
    regular_unit_tests(globals())
