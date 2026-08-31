# Copyright 2019-2026 AstroLab Software
# Author: Fabrice Jammes
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
import pytest

from fink_broker.common.spark_utils import parse_kafka_servers


def test_single_server():
    """A plain HOST:PORT gives one broker."""
    assert parse_kafka_servers("kafka-0.kafka:9092") == [("kafka-0.kafka", 9092)]


def test_several_servers_keep_their_order():
    """Every entry of the list is returned, in the order it was given."""
    servers = "kafka-1.kafka:9092,kafka-0.kafka:9093"
    assert parse_kafka_servers(servers) == [
        ("kafka-1.kafka", 9092),
        ("kafka-0.kafka", 9093),
    ]


def test_surrounding_blanks_are_ignored():
    """Blanks around an entry, and empty entries, are dropped."""
    assert parse_kafka_servers(" kafka:9092 , ,kafka:9093") == [
        ("kafka", 9092),
        ("kafka", 9093),
    ]


def test_ip_literals():
    """An IPv4 address, and a bracketed IPv6 one, are valid hosts."""
    assert parse_kafka_servers("10.0.0.1:9092") == [("10.0.0.1", 9092)]
    assert parse_kafka_servers("[::1]:9092") == [("::1", 9092)]


def test_empty_list_is_refused():
    """A list holding no entry at all is an error, not an empty result."""
    with pytest.raises(ValueError, match="No Kafka bootstrap server"):
        parse_kafka_servers(" , ")


def test_missing_port_is_refused():
    """A bare host carries no port to connect to."""
    with pytest.raises(ValueError, match="Malformed"):
        parse_kafka_servers("kafka-0.kafka")


@pytest.mark.parametrize(
    "servers",
    [
        "kafka:0",
        "kafka:65536",
        "kafka:-1",
        "kafka:9092a",
        "kafka:",
    ],
)
def test_invalid_port_is_refused(servers):
    """A port outside 1-65535, or not a number at all, is refused."""
    with pytest.raises(ValueError, match="port"):
        parse_kafka_servers(servers)


@pytest.mark.parametrize(
    "servers",
    [
        ":9092",
        "kafka_0.kafka:9092",
        "-kafka:9092",
        "kafka..kafka:9092",
        "http://kafka:9092",
        "kafka/../evil:9092",
    ],
)
def test_invalid_host_is_refused(servers):
    """A host that is neither a valid hostname nor an IP literal is refused."""
    with pytest.raises(ValueError, match="host"):
        parse_kafka_servers(servers)
