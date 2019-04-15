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
from pyspark.sql.streaming import StreamingQuery

import os
import threading
import pandas as pd

import time

from fink_broker.tester import spark_unit_tests

def recentprogress(query: StreamingQuery, colnames: list):
    """ Register recent query progresses in a Pandas DataFrame.

    It turns out that Structured Streaming cannot be monitored as Streaming
    in the Spark UI (why?), hence this simple routine to be
    able to access it in a friendly way.

    Parameters
    ----------
    query: StreamingQuery
        StreamingQuery query.
    colnames: list of str
        Fields of the query.recentProgress to be registered

    Returns
    ----------
    data: pd.DataFrame
        Pandas DataFrame whose columns are colnames, and index
        is the timestamp.

    Examples
    ----------
    Start a memory sink from a Streaming dataframe
    >>> countquery = dfstream\
.writeStream\
.queryName("monitor")\
.format("memory")\
.outputMode("update")\
.start()
    >>> time.sleep(2)

    Collect fluxes in a Pandas dataframe
    >>> colnames = ["inputRowsPerSecond", "processedRowsPerSecond", "timestamp"]
    >>> pandas_df = recentprogress(countquery, colnames)

    Stop the sink
    >>> countquery.stop()
    """
    # Force to register timestamp
    if "timestamp" not in colnames:
        colnames.append("timestamp")

    # Register fields in a dic
    dicval = {i: [] for i in colnames}
    for c in query.recentProgress:
        if len(c) == 0:
            continue
        try:
            for colname in colnames:
                dicval[colname].append(c[colname])
        except (TypeError, KeyError):
            # This can happen if the stream has not begun
            # or is stuck.
            continue

    # Build DataFrame from dic
    data = pd.DataFrame(dicval)

    if not data.empty:
        # Set timestamp as index
        data.set_index('timestamp',inplace=True)

        # Format it as datetime (useful for plot)
        data.index = pd.to_datetime(data.index)

    return data

def save_monitoring(
    path: str, outputname: str,
    query: StreamingQuery, colnames: list):
    """ Save stream progress locally (driver) into disk (CSV).

    Parameters
    ----------
    path: str
        Folder where to save the data.
    outputname: str
        Name of the file containing monitoring data. If it does not exist,
        it will be created. Data format is CSV.
    query: StreamingQuery
        Streaming query to monitor
    colnames: list of str
        Fields of the query.recentProgress to be registered

    Examples
    ----------
    Start a memory sink from a Streaming dataframe
    >>> countquery = dfstream\
.writeStream\
.queryName("monitor")\
.format("memory")\
.outputMode("update")\
.start()
    >>> time.sleep(2)

    Collect rates in a Pandas dataframe
    >>> colnames = ["inputRowsPerSecond", "processedRowsPerSecond", "timestamp"]
    >>> out = save_monitoring(".", "test.csv", countquery, colnames)

    Stop the sink
    >>> countquery.stop()
    """
    dfp = recentprogress(query, colnames)
    if dfp.empty:
        return False
    dfp.to_csv(os.path.join(path, outputname))

def monitor_progress_webui(
        countquery: StreamingQuery, tinterval: int,
        colnames: list, outpath: str, outputname:str, test: bool=False):
    """ Simple listener to Spark structured streaming.

    Data is saved at outpath/outputname.

    Pyspark does not allow to asynchronously monitor queries
    associated with a SparkSession by attaching a StreamingQueryListener,
    as would be done in Scala/Java. Therefore we provide here a custom
    function to do it.

    Parameters
    ----------
    countquery: StreamingQuery
        Streaming query to monitor
    tinterval: int
        Time interval in between two calls (second)
    colnames: list of str
        Fields of the query.recentProgress to be registered
    outpath: str
        Path to the folder where to save the progress data.
    outputname: str
        Name of the file containing monitoring data. If it does not exist,
        it will be created. Data format is CSV.
    test: bool, optional
        Set to True for canceling the daemon after its call. default is False.


    Examples
    ----------
    Start a memory sink from a Streaming dataframe
    >>> countquery = dfstream\
.writeStream\
.queryName("monitor")\
.format("memory")\
.outputMode("update")\
.start()
    >>> time.sleep(2)

    Collect rates in a Pandas dataframe
    >>> colnames = ["inputRowsPerSecond", "processedRowsPerSecond", "timestamp"]
    >>> monitor_progress_webui(countquery, 1, colnames, ".", "test.csv", True)

    Stop the sink
    >>> countquery.stop()
    """
    t = threading.Timer(
        tinterval,
        monitor_progress_webui,
        args=(countquery, tinterval, colnames, outpath, outputname)
    )

    # Start it as a daemon
    t.daemon = True
    t.start()

    # Monitor the progress of the stream, and save data for the webUI
    save_monitoring(outpath, outputname, countquery, colnames)

    if test:
        t.cancel()


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    # Run the Spark test suite
    spark_unit_tests(globals(), withstreaming=True)
