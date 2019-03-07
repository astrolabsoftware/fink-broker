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
"""Utilities for manipulating Avro data and schemas.
Some routines borrowed from lsst-dm/alert_stream and adapted.
"""
import io
import fastavro

__all__ = [
    'writeAvroData',
    'readAvroData',
    'readSchemaData',
    'readSchemaFromAvroFile',
    'decoder']

def writeAvroData(json_data: dict, json_schema: dict) -> io._io.BytesIO:
    """Encode json into Avro format given a schema.

    Parameters
    ----------
    json_data : `dict`
        The JSON data containing message content.
    json_schema : `dict`
        The writer Avro schema for encoding data.

    Returns
    -------
    `_io.BytesIO`
        Encoded data.
    """
    bytes_io = io.BytesIO()
    fastavro.schemaless_writer(bytes_io, json_schema, json_data)
    return bytes_io

def readAvroData(bytes_io: io._io.BytesIO, json_schema: dict) -> dict:
    """Read data and decode with a given Avro schema.

    Parameters
    ----------
    bytes_io : `_io.BytesIO`
        Data to be decoded.
    json_schema : `dict`
        The reader Avro schema for decoding data.

    Returns
    -------
    `dict`
        Decoded data.
    """
    bytes_io.seek(0)
    message = fastavro.schemaless_reader(bytes_io, json_schema)
    return message

def readSchemaData(bytes_io: io._io.BytesIO):
    """Read data that already has an Avro schema.

    Parameters
    ----------
    bytes_io : `_io.BytesIO`
        Data to be decoded.

    Returns
    -------
    `dict`
        Decoded data.
    """
    bytes_io.seek(0)
    message = fastavro.reader(bytes_io)
    return message

def readSchemaFromAvroFile(fn: str) -> dict:
    """ Reach schema from a binary avro file.

    Parameters
    ----------
    fn: str
        Input Avro file with schema.

    Returns
    ----------
    schema: dict
        Dictionary (JSON) describing the schema.
    """
    with open(fn, mode='rb') as file_data:
        data = readSchemaData(file_data)
        schema = data.schema
    return schema

def decoder(msg: io._io.BytesIO, alert_schema: dict) -> dict:
    """ Decode an alert from Kafka (avro format)

    Parameters
    ----------
    msg: bytes-like object (`_io.BytesIO`)
        Message coming from Kafka.
    alert_schema: dict
        Dictionary (JSON) containing the schema of the message.

    Returns
    ----------
    alert: dict
        Dictionary describing the alert.
    """
    bytes_io = io.BytesIO(msg)
    alert = readAvroData(bytes_io, alert_schema)
    return alert
