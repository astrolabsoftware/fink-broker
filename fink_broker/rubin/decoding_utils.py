#!/usr/bin/env python
# Copyright 2024
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
import io
import csv
import os
import glob
import time
import requests
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from astropy.time import Time
import confluent_kafka


def stamp_table(table, schema, name, fieldtype):
    """ """
    # Add mjd in UTC
    brokerIngestMjd = Time.now().mjd
    table = table.append_column(
        pa.field(name, fieldtype), [[brokerIngestMjd] * len(table)]
    )
    schema = schema.append(pa.field(name, fieldtype))

    return table, schema


def crossmatch_cds(table, table_schema):
    """ """
    tmp = table["diaSource"].to_numpy()
    results = cdsxmatch(
        [el["diaSourceId"] for el in tmp],
        [el["ra"] for el in tmp],
        [el["dec"] for el in tmp],
        distmaxarcsec=1.5,
        extcatalog="simbad",
        cols="main_type",
    )
    table = table.append_column(
        pa.field("cdsxmatch", pa.string()), [results.to_numpy()]
    )
    table_schema = table_schema.append(pa.field("cdsxmatch", pa.string()))

    return table, table_schema


def write_alert(msgs, table_schema_path, fs, uuid, where="rubin_kafka"):
    """Write alerts on disk

    Parameters
    ----------
    msgs: list
        Batch of alerts
    table_schema_path: str
        Folder containing LSST schemas in parquet format
    fs: optional
        Type of filesystem. None means local.
    uuid: int
        ??
    where: str
        Folder to write alerts. Depends on filesystem chosen.
    """
    pdf = pd.DataFrame.from_records(msgs)

    # Get latest schema
    with open(os.path.join(table_schema_path, "latest_schema.log"), "r") as f:
        schema_version = f.read()

    schema_files = glob.glob(
        os.path.join(table_schema_path, schema_version, "*.parquet")
    )
    table_schema = pq.read_schema(schema_files[0])

    # remove metadata for compatibility
    table_schema = table_schema.remove_metadata()
    table = pa.Table.from_pandas(pdf, schema=table_schema)

    # Add mjd in UTC
    table, table_schema = stamp_table(
        table, table_schema, "brokerIngestMjd", pa.float64()
    )

    # # Perform crossmatch
    # table, table_schema = crossmatch_cds(table, table_schema)

    # Add mjd in UTC
    table, table_schema = stamp_table(
        table, table_schema, "brokerEndProcessMjd", pa.float64()
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


def generate_csv(s: str, lists: list) -> str:
    """Make a string (CSV formatted) given lists of data and header."""
    output = io.StringIO()
    writer = csv.writer(output, quoting=csv.QUOTE_NONNUMERIC)
    _ = [writer.writerow(row) for row in zip(*lists)]
    return s + output.getvalue().replace("\r", "")


def cdsxmatch(
    objectId, ra, dec, distmaxarcsec: float, extcatalog: str, cols: str
) -> pd.Series:
    """Query the CDSXmatch service to find identified objects"""
    # If nothing
    if len(ra) == 0:
        return ""

    # Catch TimeoutError and ConnectionError
    try:
        # Build a catalog of alert in a CSV-like string
        table_header = """ra_in,dec_in,objectId\n"""
        table = generate_csv(table_header, [ra, dec, objectId])

        # Send the request!
        r = requests.post(
            "http://cdsxmatch.u-strasbg.fr/xmatch/api/v1/sync",
            data={
                "request": "xmatch",
                "distMaxArcsec": distmaxarcsec,
                "selection": "all",
                "RESPONSEFORMAT": "csv",
                "cat2": extcatalog,
                "cols2": cols,
                "colRA1": "ra_in",
                "colDec1": "dec_in",
            },
            files={"cat1": table},
        )

        if r.status_code != 200:
            names = ["Fail {}".format(r.status_code)] * len(objectId)
            return pd.Series(names)
        else:
            cols = cols.split(",")
            pdf = pd.read_csv(io.BytesIO(r.content))

            if pdf.empty:
                name = ",".join(["Unknown"] * len(cols))
                names = [name] * len(objectId)
                return pd.Series(names)

            # join
            pdf_in = pd.DataFrame({"objectId_in": objectId})
            pdf_in.index = pdf_in["objectId_in"]

            # Remove duplicates (keep the one with minimum distance)
            pdf_nodedup = pdf.loc[pdf.groupby("objectId").angDist.idxmin()]
            pdf_nodedup.index = pdf_nodedup["objectId"]

            pdf_out = pdf_in.join(pdf_nodedup)

            # only for SIMBAD as we use `main_type` for our classification
            if "main_type" in pdf_out.columns:
                pdf_out["main_type"] = pdf_out["main_type"].replace(np.nan, "Unknown")

            if len(cols) > 1:
                # Concatenate all columns in one
                # use comma-separated values
                cols = [i.strip() for i in cols]
                pdf_out = pdf_out[cols]
                pdf_out["concat_cols"] = pdf_out.apply(
                    lambda x: ",".join(x.astype(str).to_numpy().tolist()), axis=1
                )
                return pdf_out["concat_cols"]
            elif len(cols) == 1:
                # single column to return
                return pdf_out[cols[0]].astype(str)

    except (ConnectionError, TimeoutError, ValueError):
        ncols = len(cols.split(","))
        name = ",".join(["Fail"] * ncols)
        names = [name] * len(objectId)
        return pd.Series(names)


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
