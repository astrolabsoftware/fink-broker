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
"""Utilities for manipulating Avro data and schemas.
Some routines borrowed from lsst-dm/alert_stream and adapted.
"""
import io
import os
import fastavro

from fink_broker.tester import regular_unit_tests

__all__ = [
    'writeavrodata',
    'readschemadata',
    'readschemafromavrofile']

def writeavrodata(json_data: dict, json_schema: dict) -> io._io.BytesIO:
    """ Encode json into Avro format given a schema.

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

    Examples
    ----------
    >>> with open(ztf_alert_sample, mode='rb') as file_data:
    ...   data = readschemadata(file_data)
    ...   # Read the schema
    ...   schema = data.schema
    ...   for record in data:
    ...     bytes = writeavrodata(record, schema)
    >>> print(type(bytes))
    <class '_io.BytesIO'>
    """
    bytes_io = io.BytesIO()
    fastavro.schemaless_writer(bytes_io, json_schema, json_data)
    return bytes_io

def readschemadata(bytes_io: io._io.BytesIO) -> fastavro._read.reader:
    """Read data that already has an Avro schema.

    Parameters
    ----------
    bytes_io : `_io.BytesIO`
        Data to be decoded.

    Returns
    -------
    `fastavro._read.reader`
        Iterator over records (`dict`) in an avro file.

    Examples
    ----------
    Open an avro file, and read the schema and the records
    >>> with open(ztf_alert_sample, mode='rb') as file_data:
    ...   data = readschemadata(file_data)
    ...   # Read the schema
    ...   schema = data.schema
    ...   # data is an iterator
    ...   for record in data:
    ...     print(type(record))
    <class 'dict'>
    """
    bytes_io.seek(0)
    message = fastavro.reader(bytes_io)
    return message

def readschemafromavrofile(fn: str) -> dict:
    """ Reach schema from a binary avro file.

    Parameters
    ----------
    fn: str
        Input Avro file with schema.

    Returns
    ----------
    schema: dict
        Dictionary (JSON) describing the schema.

    Examples
    ----------
    >>> schema = readschemafromavrofile(ztf_alert_sample)
    >>> print(schema['version'])
    3.3
    """
    with open(fn, mode='rb') as file_data:
        data = readschemadata(file_data)
        schema = data.writer_schema
    return schema


if __name__ == "__main__":
    """ Execute the test suite """
    # Add sample file to globals
    globs = globals()
    root = os.environ['FINK_HOME']
    globs["ztf_alert_sample"] = os.path.join(
        root, "fink-alert-schemas/ztf/template_schema_ZTF_3p3.avro")

    # Run the regular test suite
    regular_unit_tests(globs)
