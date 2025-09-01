#!/usr/bin/env python
# Copyright 2019-2025
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
import os
import glob
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from astropy.time import Time
import confluent_kafka


def add_constant_field_to_table(fieldname, fieldvalue, fieldtype, table, schema):
    """Add constant value field in the alert packet & schema

    Parameters
    ----------
    fieldname: str
        Name for the new field
    fieldvalue: Any
        Value of the new field
    fieldtype: pyarrow.lib.DataType
        Pyarrow data type for the new field
    table: pyarrow.Table
        Pyarrow table with alert data
    schema: pyarrow.Schema
        Table schema

    Returns
    -------
    table: pyarrow.Table
        Pyarrow table updated
    schema: pyarrow.Schema
        Table schema updated
    """
    table = table.append_column(
        pa.field(fieldname, fieldtype), [[fieldvalue] * len(table)]
    )
    schema = schema.append(pa.field(fieldname, fieldtype))

    return table, schema


def write_alert(
    msgs, table_schema_path, avro_schema=None, fs=None, uuid=0, where="rubin_kafka"
):
    """Write alerts on disk

    Parameters
    ----------
    msgs: list
        Batch of alerts
    table_schema_path: str
        Folder containing LSST schemas in parquet format
    avro_schema: str
        Path to an avro file used to simulate streams.
        ONLY USE THIS in the Continuous integration.
        Default is None, meaning it is not used.
    fs: optional
        Type of filesystem. None (default for CI) means local.
        Production uses HadoopFileSystem.
    uuid: int
        Extra uuid when writing parquet files on disk.
    where: str
        Folder to write alerts. Depends on filesystem chosen.
    """
    if avro_schema is not None:
        # This is a workaround to work in CI with
        # fixed test data & schema
        import io
        import fastavro
        from fink_alert_simulator.avroUtils import readschemafromavrofile

        schema = readschemafromavrofile(avro_schema)
        msgs = [fastavro.schemaless_reader(io.BytesIO(m), schema) for m in msgs]
    pdf = pd.DataFrame.from_records(msgs)

    # Get schema
    if avro_schema is not None:
        # Change this when re-creating test data
        schema_version = "lsst.v7_4.parquet"
    else:
        with open(os.path.join(table_schema_path, "latest_schema.txt"), "r") as f:
            schema_version = f.read()

    schema_files = glob.glob(
        os.path.join(table_schema_path, schema_version, "*.parquet")
    )
    table_schema = pq.read_schema(schema_files[0])

    # remove metadata for compatibility
    table_schema = table_schema.remove_metadata()
    table = pa.Table.from_pandas(pdf, schema=table_schema)

    # Add additional fields
    table, table_schema = add_constant_field_to_table(
        "brokerIngestMjd",
        Time.now().mjd,
        pa.float64(),
        table,
        table_schema,
    )

    # FIXME: check carefuly that the schema version
    # is in the form lsst.v{}_{}
    table, table_schema = add_constant_field_to_table(
        "lsst_schema_version",
        schema_version.split(".parquet")[0],
        pa.string(),
        table,
        table_schema,
    )

    # Save on disk
    filename = "part-{}-{}-{{i}}-{}.parquet".format(os.getpid(), os.getppid(), uuid)
    pq.write_to_dataset(
        table,
        where,
        schema=table_schema,
        basename_template=filename,
        existing_data_behavior="overwrite_or_ignore",
        filesystem=fs,
    )


def return_offsets(
    consumer, topic, waitfor=1, timeout=10, hide_empty_partition=True, verbose=False
):
    """Poll servers to get the total committed offsets, and remaining lag

    Parameters
    ----------
    consumer: confluent_kafka.Consumer
        Kafka consumer
    topic: str
        Topic name
    waitfor: int, optional
        Time in second to wait before polling. Default is 1 second.
    timeout: int, optional
        Timeout in second when polling the servers. Default is 10.
    hide_empty_partition: bool, optional
        If True, display only non-empty partitions.
        Default is True
    verbose: bool, optional
        If True, prints useful table. Default is False.

    Returns
    -------
    total_offsets: int
        Total number of messages committed across all partitions
    total_lag: int
        Remaining messages in the topic across all partitions.
    """
    time.sleep(waitfor)
    # Get the topic's partitions
    metadata = consumer.list_topics(topic, timeout=timeout)
    if metadata.topics[topic].error is not None:
        raise confluent_kafka.KafkaException(metadata.topics[topic].error)

    # Construct TopicPartition list of partitions to query
    partitions = [
        confluent_kafka.TopicPartition(topic, p)
        for p in metadata.topics[topic].partitions
    ]

    # Query committed offsets for this group and the given partitions
    try:
        committed = consumer.committed(partitions, timeout=timeout)
    except confluent_kafka.KafkaException as exception:
        kafka_error = exception.args[0]
        if kafka_error.code() == confluent_kafka.KafkaError._TIMED_OUT:
            return -1, -1
        else:
            return 0, 0

    total_offsets = 0
    total_lag = 0
    if verbose:
        print("%-50s  %9s  %9s" % ("Topic [Partition]", "Committed", "Lag"))
        print("=" * 72)
    for partition in committed:
        # Get the partitions low and high watermark offsets.
        (lo, hi) = consumer.get_watermark_offsets(
            partition, timeout=timeout, cached=False
        )

        if partition.offset == confluent_kafka.OFFSET_INVALID:
            offset = "-"
        else:
            offset = "%d" % (partition.offset)

        if hi < 0:
            lag = 0  # Unlikely
        elif partition.offset < 0:
            # No committed offset, show total message count as lag.
            # The actual message count may be lower due to compaction
            # and record deletions.
            lag = hi - lo
            partition.offset = 0
        else:
            lag = hi - partition.offset
        #
        total_offsets = total_offsets + partition.offset
        total_lag = total_lag + int(lag)

        if verbose:
            if (hide_empty_partition and (offset != "-" or int(lag) > 0)) or (
                not hide_empty_partition
            ):
                print(
                    "%-50s  %9s  %9s"
                    % (
                        "{} [{}]".format(partition.topic, partition.partition),
                        offset,
                        lag,
                    )
                )
    if verbose:
        print("-" * 72)
        print(
            "%-50s  %9s  %9s" % ("Total for {}".format(topic), total_offsets, total_lag)
        )
        print("-" * 72)

    return total_offsets, total_lag
