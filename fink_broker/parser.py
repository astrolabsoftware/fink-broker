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
import argparse

def getargs(parser: argparse.ArgumentParser) -> argparse.Namespace:
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
        '-startingoffsets_stream', type=str, default='',
        help="""From which stream offset you want to start pulling data when
        building the raw database: latest, earliest, or custom.
        [KAFKA_STARTING_OFFSET]
        """)
    parser.add_argument(
        '-rawdatapath', type=str, default='',
        help="""
        Directory on disk for saving raw alert data.
        [FINK_ALERT_PATH]
        """)
    parser.add_argument(
        '-checkpointpath_raw', type=str, default='',
        help="""
        The location where the system will write all the checkpoint information
        for the raw datable. This shoul be a directory in an HDFS-compatible
        fault-tolerant file system.
        See conf/fink.conf & https://spark.apache.org/docs/latest/
        structured-streaming-programming-guide.html#starting-streaming-queries
        [FINK_ALERT_CHECKPOINT_RAW]
        """)
    parser.add_argument(
        '-checkpointpath_sci', type=str, default='',
        help="""
        The location where the system will write all the checkpoint information
        for the science datable. This should be a directory in an
        HDFS-compatible fault-tolerant file system.
        See conf/fink.conf & https://spark.apache.org/docs/latest/
        structured-streaming-programming-guide.html#starting-streaming-queries
        [FINK_ALERT_CHECKPOINT_SCI]
        """)
    parser.add_argument(
        '-science_db_name', type=str, default='',
        help="""
        The name of the HBase table
        [SCIENCE_DB_NAME]
        """)
    parser.add_argument(
        '-science_db_catalog', type=str, default='',
        help="""
        The path of HBase catalog
        [SCIENCE_DB_CATALOG]
        """)
    parser.add_argument(
        '-log_level', type=str, default='',
        help="""
        The minimum level of log: OFF, DEBUG, INFO, WARN, ERROR, CRITICAL
        [LOG_LEVEL]
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
        '-datasimpath', type=str, default='',
        help="""
        Folder containing simulated alerts to be published by Kafka.
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
    parser.add_argument(
        '-distribution_schema', type=str, default='',
        help="""
        The path where the avro schema for alert distribution is stored
        [DISTRIBUTION_SCHEMA]
        """)
    parser.add_argument(
        '-startingOffset_dist', type=str, default='',
        help="""From which offset(timestamp) you want to start the
        distribution service.
        Options are: latest, earliest or a custom timestamp
        [DISTRIBUTION_OFFSET]
        """)
    parser.add_argument(
        '-checkpointpath_dist', type=str, default='',
        help="""
        The path of file in which to store the offset for distribution service.
        This file will store the timestamp up-till which the science db is
        scanned and alerts have been distributed.
        [DISTRIBUTION_OFFSET_FILE]
        """)
    parser.add_argument(
        '-distribution_rules_xml', type=str, default='',
        help="""
        The path to distribution-rules.xml which stores user defined rules to
        filter the distribution stream
        [DISTRIBUTION_RULES_XML]
        """)
    args = parser.parse_args(None)
    return args


if __name__ == "__main__":
    """ Execute the test suite """

    # Run the regular test suite
    regular_unit_tests(globals())
