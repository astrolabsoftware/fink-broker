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
from datetime import datetime, timedelta, timezone

import pytest

from fink_broker.common import spark_utils
from fink_broker.common.spark_utils import parse_kafka_servers, probe_path


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


def test_s3_prefix_is_probed_at_the_bucket_root():
    """An object store answers for its bucket, not for a prefix.

    A prefix that does not exist yet is the normal state before the upstream
    job writes it, and listing it would raise until the timeout.
    """
    assert probe_path("s3a", "/online/raw/20240101") == "/"
    assert probe_path("s3", "/online") == "/"


def test_bucket_uri_carries_no_path():
    """`s3a://bucket` has an empty path component."""
    assert probe_path("s3a", "") == "/"


def test_other_filesystems_probe_the_path_itself():
    """On HDFS a missing path is answered with a plain False, not an error."""
    assert probe_path("hdfs", "/user/185") == "/user/185"
    assert probe_path(None, "/tmp/fink") == "/tmp/fink"


def test_a_pathless_uri_probes_the_root():
    """A URI with no path at all still gives a valid Hadoop Path."""
    assert probe_path("hdfs", "") == "/"


def test_sleep_is_clamped_to_the_deadline(monkeypatch):
    """Waiting never overshoots the caller's deadline."""
    slept = []
    monkeypatch.setattr(spark_utils.time, "sleep", slept.append)

    deadline = datetime.now(timezone.utc) + timedelta(seconds=3)
    spark_utils.sleep_before_retry(60, deadline)

    assert slept == pytest.approx([3], abs=0.5)


def test_sleep_past_the_deadline_returns_at_once(monkeypatch):
    """A deadline already reached leaves no time to wait."""
    slept = []
    monkeypatch.setattr(spark_utils.time, "sleep", slept.append)

    deadline = datetime.now(timezone.utc) - timedelta(seconds=10)
    spark_utils.sleep_before_retry(60, deadline)

    assert slept == [0]


def test_sleep_without_deadline_is_left_alone(monkeypatch):
    """Waiting is unbounded when the caller gave no deadline."""
    slept = []
    monkeypatch.setattr(spark_utils.time, "sleep", slept.append)

    spark_utils.sleep_before_retry(60, None)

    assert slept == [60]


def test_sleep_returns_a_longer_wait(monkeypatch):
    """The next attempt waits longer, whatever the clamping did."""
    monkeypatch.setattr(spark_utils.time, "sleep", lambda _: None)

    deadline = datetime.now(timezone.utc) + timedelta(seconds=1)

    assert spark_utils.sleep_before_retry(10, deadline) == pytest.approx(12)
