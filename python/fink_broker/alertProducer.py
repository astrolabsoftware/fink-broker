# Copyright 2018 AstroLab Software
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
import asyncio

from fink_broker import avroUtils

__all__ = ['AlertProducer', 'delay', 'schedule_delays']

@asyncio.coroutine
def delay(wait_sec, function, *args):
    """Sleep for a given time before calling a function.
    Parameters
    ----------
    wait_sec
        Time in seconds to sleep before calling `function`.
    function
        Function to return after sleeping.
    """
    yield from asyncio.sleep(wait_sec)
    return function(*args)


@asyncio.coroutine
def schedule_delays(eventloop, function, argslist, interval=39):
    """Schedule delayed calls of functions at a repeating interval.
    Parameters
    ----------
    eventloop
        Event loop returned by asyncio.get_event_loop().
    function
        Function to be scheduled.
    argslist
        List of inputs for function to loop over.
    interval
        Time in seconds between calls.
    """
    counter = 1
    for arg in argslist:
        wait_time = interval - (time.time() % interval)
        yield from asyncio.ensure_future(delay(wait_time, function, arg))
        print('visits finished: {} \t time: {}'.format(counter, time.time()))
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
    """
    def __init__(self, topic: str, schema_files: list=None, **kwargs):
        self.producer = confluent_kafka.Producer(**kwargs)
        self.topic = topic
        # if schema_files is not None:
        #     self.alert_schema = avroUtils.combineSchemas(schema_files)

    def send(self, data: dict, alert_schema: dict=None, encode: bool=False):
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
