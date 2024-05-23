# Copyright 2019-2024 AstroLab Software
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
    """Parse command line arguments for fink services

    Parameters
    ----------
    parser: argparse.ArgumentParser
        Empty parser

    Returns
    -------
    args: argparse.Namespace
        Object containing CLI arguments parsed

    Examples
    --------
    >>> import argparse
    >>> parser = argparse.ArgumentParser(description=__doc__)
    >>> args = getargs(parser)
    >>> print(type(args))
    <class 'argparse.Namespace'>
    """
    parser.add_argument(
        "-servers",
        type=str,
        default="",
        help="""
        Hostname or IP and port of Kafka broker producing stream.
        [KAFKA_IPPORT/KAFKA_IPPORT_SIM]
        """,
    )
    parser.add_argument(
        "-topic",
        type=str,
        default="",
        help="""
        Name of Kafka topic stream to read from.
        [KAFKA_TOPIC/KAFKA_TOPIC_SIM]
        """,
    )
    parser.add_argument(
        "-schema",
        type=str,
        default="",
        help="""
        Schema to decode the alert. Should be avro file.
        [FINK_ALERT_SCHEMA]""",
    )
    parser.add_argument(
        "-startingoffsets_stream",
        type=str,
        default="",
        help="""From which stream offset you want to start pulling data when
        building the raw database: latest, earliest, or custom.
        [KAFKA_STARTING_OFFSET]
        """,
    )
    parser.add_argument(
        "-max_offsets_per_trigger",
        type=int,
        default=5000,
        help="""Maximum number of offsets to fetch per trigger.
        Default is 5000.
        [MAX_OFFSETS_PER_TRIGGER]
        """,
    )
    parser.add_argument(
        "-online_data_prefix",
        type=str,
        default="",
        help="""Path prefix to store online data, e.g. /path/to/online.
        This would then contain automatically {raw, science}/year=/month=/day=
        [ONLINE_DATA_PREFIX]
        """,
    )
    parser.add_argument(
        "-agg_data_prefix",
        type=str,
        default="",
        help="""Path prefix to store archive data, e.g. /path/to/archive.
        This would then contain automatically {raw, science}/year=/month=/day=
        [AGG_DATA_PREFIX]
        """,
    )
    parser.add_argument(
        "-science_db_name",
        type=str,
        default="",
        help="""
        The name of the HBase table
        [SCIENCE_DB_NAME]
        """,
    )
    parser.add_argument(
        "-science_db_catalogs",
        type=str,
        default="",
        help="""
        The path for HBase table catalogs. Must exist.
        [SCIENCE_DB_CATALOGS]
        """,
    )
    parser.add_argument(
        "-log_level",
        type=str,
        default="",
        help="""
        The minimum level of log: OFF, DEBUG, INFO, WARN, ERROR, CRITICAL
        [LOG_LEVEL]
        """,
    )
    parser.add_argument(
        "-finkwebpath",
        type=str,
        default="",
        help="""
        Folder to store UI data for display.
        [FINK_UI_PATH]
        """,
    )
    parser.add_argument(
        "-tinterval",
        type=int,
        default=0,
        help="""
        Time interval between two monitoring. In seconds.
        [FINK_TRIGGER_UPDATE]
        """,
    )
    parser.add_argument(
        "-tinterval_kafka",
        type=float,
        default=0.0,
        help="""
        Time interval between two messages are published. In seconds.
        [TIME_INTERVAL]
        """,
    )
    parser.add_argument(
        "-exit_after",
        type=int,
        default=64800,
        help="""
        Stop the service after `exit_after` seconds.
        This primarily for use on CI, to stop service after some time.
        Use that with `fink start service --exit_after <time>`. Default is 24h.
        """,
    )
    parser.add_argument(
        "-datasimpath",
        type=str,
        default="",
        help="""
        Folder containing simulated alerts to be published by Kafka.
        [FINK_DATA_SIM]
        """,
    )
    parser.add_argument(
        "-poolsize",
        type=int,
        default=5,
        help="""
        Maximum number of alerts to send. If the poolsize is
        bigger than the number of alerts in `datapath`, then we replicate
        the alerts. Default is 5.
        [POOLSIZE]
        """,
    )
    parser.add_argument(
        "-distribution_servers",
        type=str,
        default="",
        help="""
        Kafka bootstrap servers for alert redistribution
        [DISTRIBUTION_SERVERS]
        """,
    )
    parser.add_argument(
        "-distribution_topic",
        type=str,
        default="",
        help="""
        Kafka topic for Alert redistribution
        [DISTRIBUTION_TOPIC]
        """,
    )
    parser.add_argument(
        "-distribution_schema",
        type=str,
        default="",
        help="""
        The path where the avro schema for alert distribution is stored
        [DISTRIBUTION_SCHEMA]
        """,
    )
    parser.add_argument(
        '-kafka_sasl_username', type=str, default='',
        help="""
        the sasl username authentication for kafka producer
        [KAFKA_SASL_USERNAME]
        """)
    parser.add_argument(
        '-kafka_sasl_password', type=str, default='',
        help="""
        the sasl password authentication for kafka producer
        [KAFKA_SASL_PASSWORD]
        """)
    parser.add_argument(
        '-kafka_buffer_memory', type=int, default=134217728,
        help="""
        the kafka buffer memory size
        [KAFKA_BUFFER_MEMORY]
        """)
    parser.add_argument(
        '-kafka_delivery_timeout_ms', type=int, default=1000,
        help="""
        the delivery kafka message timeout in ms
        [KAFKA_DELIVERY_TIMEOUT_MS]
        """)
    parser.add_argument(
        '-startingOffset_dist', type=str, default='',
        help="""From which offset(timestamp) you want to start the
        distribution service.
        Options are: latest, earliest or a custom timestamp
        [DISTRIBUTION_OFFSET]
        """,
    )
    parser.add_argument(
        "-checkpointpath_dist",
        type=str,
        default="",
        help="""
        The path of file in which to store the offset for distribution service.
        This file will store the timestamp up-till which the science db is
        scanned and alerts have been distributed.
        [DISTRIBUTION_OFFSET_FILE]
        """,
    )
    parser.add_argument(
        "-distribution_rules_xml",
        type=str,
        default="",
        help="""
        The path to distribution-rules.xml which stores user defined rules to
        filter the distribution stream
        [DISTRIBUTION_RULES_XML]
        """,
    )
    parser.add_argument(
        "-slack_channels",
        type=str,
        default="",
        help="""
        Text file with list of slack channels to which automatic alerts
        must be sent for e.g. based on cross-match type
        [SLACK_CHANNELS]
        """,
    )
    parser.add_argument(
        "-night",
        type=str,
        default="",
        help="""
        YYYYMMDD night
        [NIGHT]
        """,
    )
    parser.add_argument(
        "-fs",
        type=str,
        default="",
        help="""
        Filesystem: local or hdfs.
        [FS_KIND]
        """,
    )
    parser.add_argument(
        "-datapath",
        type=str,
        default="",
        help="""
        Directory on disk for saving temporary alert data.
        [DATA_PREFIX]
        """,
    )
    parser.add_argument(
        "--save_science_db_catalog_only",
        action="store_true",
        help="""
        If True, save only the catalog on disk and do not push
        data on HBase. Default is False.
        [SAVE_SCIENCE_DB_CATALOG_ONLY]
        """,
    )
    parser.add_argument(
        "-index_table",
        type=str,
        default="",
        help="""
        Name of the rowkey for index table
        [INDEXTABLE]
        """,
    )
    parser.add_argument(
        "-tns_folder",
        type=str,
        default="",
        help="""
        Folder to store logs and keys for TNS submission
        [TNS_FOLDER]
        """,
    )
    parser.add_argument(
        "--tns_sandbox",
        action="store_true",
        help="""
        If True, push to TNS sandbox. Default is False.
        [TNS_SANDBOX]
        """,
    )
    parser.add_argument(
        "-substream_prefix",
        type=str,
        default="fink_",
        help="""
        Prefix for outgoing substreams
        [SUBSTREAM_PREFIX]
        """,
    )
    parser.add_argument(
        "-fink_fat_output",
        type=str,
        default="",
        help="""
        Folder that contains fink-fat output parquet files
        [FINK_FAT_OUTPUT]
        """,
    )
    parser.add_argument(
        '-mmconfigpath', type=str, default='no-config',
        help="""
        Path to fink_mm configuration file
        [MMCONFIGPATH]
"""
    )
    parser.add_argument(
        '-producer', type=str, default='ztf',
        help="""
        Name of the alert producer. Currently available: ztf, elasticc, sims
        [PRODUCER]
        """,
    )
    parser.add_argument(
        "--noscience",
        action="store_true",
        help="""
        Disable execution of science modules
        """,
    )
    parser.add_argument(
        "-tns_raw_output",
        type=str,
        default="",
        help="""
        Folder that contains raw TNS catalog
        [TNS_RAW_OUTPUT]
        """,
    )
    args = parser.parse_args(None)
    return args


if __name__ == "__main__":
    """ Execute the test suite """

    # Run the regular test suite
    regular_unit_tests(globals())
