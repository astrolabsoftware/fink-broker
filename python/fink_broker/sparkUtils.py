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
from pyspark import SparkContext
from pyspark.sql.column import Column, _to_java_column

def from_avro(dfcol, jsonFormatSchema):
    """ Decode the Avro data contained in a DataFrame column into a struct.

    Note:
    Pyspark does not have all features contained in Spark core (Scala), hence
    we provide here a wrapper around the Scala function `from_avro`.
    You need to have the package org.apache.spark:spark-avro_2.11:2.x.y in the
    classpath to have access to it from the JVM.

    Parameters
    ----------
    dfcol: Column
        DataFrame Column with encoded Avro data (binary). Typically this is
        what comes from reading stream from Kafka.
    jsonFormatSchema: str
        Avro schema in JSON string format.

    Returns
    ----------
    out: Column
        DataFrame Column with decoded Avro data.
    """
    sc = SparkContext._active_spark_context
    avro = sc._jvm.org.apache.spark.sql.avro
    f = getattr(getattr(avro, "package$"), "MODULE$").from_avro
    return Column(f(_to_java_column(dfcol), jsonFormatSchema))

def writeToCsv(batchDF, batchId):
    """ Write DataFrame data into a CSV file.

    The only supported Output Modes for File Sink is `Append`, but we need the
    complete table updated and dumped on disk here.
    Therefore this routine allows us to use CSV file sink with `Complete`
    output mode.

    TODO: that would be great to generalise this method!
    Get rid of these hardcoded paths!

    Parameters
    ----------
    batchDF: DataFrame
        Static Spark DataFrame with stream data
    batchId: int
        ID of the batch (from 0 to N).
    """
    batchDF.select(["type", "count"])\
        .toPandas()\
        .to_csv("web/data/simbadtype.csv", index=False)
    batchDF.unpersist()

def quiet_logs(sc, log_level="ERROR"):
    """ Set the level of log in Spark.

    Parameters
    ----------
    sc : SparkContext
        The SparkContext for the session
    log_level : String [optional]
        Level of log wanted: INFO, WARN, ERROR, OFF, etc.
    """
    ## Get the logger
    logger = sc._jvm.org.apache.log4j

    ## Set the level
    level = getattr(logger.Level, log_level, "INFO")

    logger.LogManager.getLogger("org"). setLevel(level)
    logger.LogManager.getLogger("akka").setLevel(level)
