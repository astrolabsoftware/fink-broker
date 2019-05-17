# Copyright 2019 AstroLab Software
# Author: Maria Patterson, Julien Peloton
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
import confluent_kafka
import time
import os
import asyncio

from typing import Any

from fink_broker import avroUtils
from fink_broker.tester import regular_unit_tests

__all__ = ['AlertProducer', 'delay', 'schedule_delays']

@asyncio.coroutine
def delay(wait_sec: float, function: function, *args) -> Any: # NOSONAR
    """Sleep for a given time before calling a function.

    NOTE: we are mixing the use of yield from & return here for good reasons.
    But apparently sonarqube is not happy with it. After thinking, it makes
    all sense to use both here, hence the NOSONAR keywords.

    Parameters
    ----------
    wait_sec: float
        Time in seconds to sleep before calling `function`.
    function: function
        Function to return after sleeping.

    Returns
    ----------
    out: Any
        The output of the user-defined `function(*args)`

    Examples
    ----------
    """
    yield from asyncio.sleep(wait_sec) # NOSONAR
    return function(*args) # NOSONAR

@asyncio.coroutine
def schedule_delays(
        eventloop: asyncio.unix_events._UnixSelectorEventLoop,
        function: function, argslist: list, interval: int = 39.0):
    """Schedule delayed calls of functions at a repeating interval.

    Parameters
    ----------
    eventloop: asyncio.unix_events._UnixSelectorEventLoop
        Event loop returned by asyncio.get_event_loop().
    function: function
        Function to be scheduled.
    argslist: list
        List of inputs for function to loop over.
    interval: float
        Time in seconds between calls.

    Examples
    ----------
    >>> def return_num(num: int) -> int:
    ...   return num

    >>> loop = asyncio.get_event_loop()
    >>> g = asyncio.ensure_future(schedule_delays(loop, return_num, (14,15), 1))
    >>> loop.run_forever()
    Alert sent: 1
    Alert sent: 2
    """
    counter = 1
    for arg in argslist:
        wait_time = interval - (time.time() % interval)
        yield from asyncio.ensure_future(delay(wait_time, function, arg))
        print('Alert sent: {}'.format(counter))
        counter += 1
    eventloop.stop()

class AlertProducer(object):
    """Alert stream producer with Kafka.

    Parameters
    ----------
    topic : `str`
        The name of the topic stream for writing.
    schema_files : List of str, optional
        List with paths to avro schemas for encoding data.
        Shemas will be combined in one. Default is None.
    **kwargs
        Keyword arguments for configuring confluent_kafka.Producer().

    Examples
    ----------
    Define the properties of the producer. Here there should be a local Kafka
    cluster running, with port 29092.
    >>> conf = {'bootstrap.servers': 'localhost:29092'}
    >>> streamProducer = AlertProducer(
    ...   "test-topic", schema_files=None, **conf)

    Open a file on disk, and send its data via the producer.
    >>> with open(ztf_alert_sample, mode='rb') as file_data:
    ...   data = avroUtils.readschemadata(file_data)
    ...   schema = data.schema
    ...   for record in data:
    ...     streamProducer.send(record, alert_schema=schema, encode=True)
    >>> g = streamProducer.flush()
    """
    def __init__(self, topic: str, schema_files: list = None, **kwargs):
        self.producer = confluent_kafka.Producer(**kwargs)
        self.topic = topic

    def send(self, data: dict, alert_schema: dict = None, encode: bool = False):
        """Sends a message to Kafka stream.

        You can choose to encode or not the message (using avro).
        If you choose to encode the message, you need to specify the schema.
        The schema can be specified when you instantiate the `AlertProducer`
        class (using `schema_files`), or when you send the
        message (`alert_schema`).

        Parameters
        ----------
        data : dict
            Data containing message content. If encode is True, expects JSON.
        alert_schema: dict, optional
            Avro schema for encoding data. Default is None.
        encode : `boolean`, optional
            If True, encodes data to Avro format. If False, sends data raw.
            Default is False.
        """
        if encode is True:
            if alert_schema is None:
                avro_bytes = avroUtils.writeavrodata(data, self.alert_schema)
            else:
                avro_bytes = avroUtils.writeavrodata(data, alert_schema)
            raw_bytes = avro_bytes.getvalue()
            self.producer.produce(self.topic, raw_bytes)
        else:
            data_str = "{}".format(data)
            self.producer.produce(self.topic, data_str)

    def flush(self):
        """ Publish message to the Kafka cluster.
        """
        return self.producer.flush()


if __name__ == "__main__":
    """ Execute the test suite """
    # Add sample file to globals
    globs = globals()
    root = os.environ['FINK_HOME']
    globs["ztf_alert_sample"] = os.path.join(
        root, "schemas/template_schema_ZTF.avro")

    # Run the regular test suite
    regular_unit_tests(globs)
