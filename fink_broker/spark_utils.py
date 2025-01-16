# Copyright 2019-2024 AstroLab Software
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
import time
import logging
from typing import Tuple
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.types import StructType

import os
import json

from fink_broker.avro_utils import readschemafromavrofile
from fink_broker.tester import spark_unit_tests

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_LOG = logging.getLogger(__name__)


def from_avro(dfcol: Column, jsonformatschema: str) -> Column:
    """Decode the Avro data contained in a DataFrame column into a struct.

    Note:
    Pyspark does not have all features contained in Spark core (Scala), hence
    we provide here a wrapper around the Scala function `from_avro`.
    You need to have the package org.apache.spark:spark-avro_2.11:2.x.y in the
    classpath to have access to it from the JVM.

    Parameters
    ----------
    dfcol: Column
        Streaming DataFrame Column with encoded Avro data (binary).
        Typically this is what comes from reading stream from Kafka.
    jsonformatschema: str
        Avro schema in JSON string format.

    Returns
    -------
    out: Column
        DataFrame Column with decoded Avro data.

    Examples
    --------
    >>> _, _, alert_schema_json = get_schemas_from_avro(ztf_avro_sample)

    >>> df_decoded = dfstream.select(
    ...   from_avro(dfstream["value"], alert_schema_json).alias("decoded"))
    >>> query = df_decoded.writeStream.queryName("qraw").format("memory")
    >>> t = query.outputMode("update").start()
    >>> t.stop()
    """
    sc = SparkContext._active_spark_context
    avro = sc._jvm.org.apache.spark.sql.avro
    f = getattr(getattr(avro, "package$"), "MODULE$").from_avro
    return Column(f(_to_java_column(dfcol), jsonformatschema))


def to_avro(dfcol: Column) -> Column:
    """Serialize the structured data of a DataFrame column into avro data (binary).

    Note:
    Since Pyspark does not have a function to convert a column to and from
    avro data, this is a wrapper around the scala function 'to_avro'.
    Just like the function above, to be able to use this you need to have
    the package org.apache.spark:spark-avro_2.11:2.x.y in the classpath.

    Parameters
    ----------
    dfcol: Column
        A DataFrame Column with Structured data

    Returns
    -------
    out: Column
        DataFrame Column encoded into avro data (binary).
        This is what is required to publish to Kafka Server for distribution.

    Examples
    --------
    >>> from pyspark.sql.functions import col, struct
    >>> avro_example_schema = '''
    ... {
    ...     "type" : "record",
    ...     "name" : "struct",
    ...     "fields" : [
    ...             {"name" : "col1", "type" : "long"},
    ...             {"name" : "col2", "type" : "string"}
    ...     ]
    ... }'''
    >>> df = spark.range(5)
    >>> df = df.select(struct("id",\
                 col("id").cast("string").alias("id2"))\
                 .alias("struct"))
    >>> avro_df = df.select(to_avro(col("struct")).alias("avro"))
    """
    sc = SparkContext._active_spark_context
    avro = sc._jvm.org.apache.spark.sql.avro
    f = getattr(getattr(avro, "package$"), "MODULE$").to_avro
    return Column(f(_to_java_column(dfcol)))


def write_to_csv(batchdf: DataFrame, batchid: int, fn: str = "web/data/simbadtype.csv"):
    """Write DataFrame data into a CSV file.

    The only supported Output Modes for File Sink is `Append`, but we need the
    complete table updated and dumped on disk here.
    Therefore this routine allows us to use CSV file sink with `Complete`
    output mode.

    TODO: that would be great to generalise this method!
    Get rid of these hardcoded paths!

    Parameters
    ----------
    batchdf: DataFrame
        Static Spark DataFrame with stream data. Expect 2 columns
        with variable names and their count.
    batchid: int
        ID of the batch.
    fn: str, optional
        Filename for storing the output.

    Examples
    --------
    >>> rdd = spark.sparkContext.parallelize(zip([1, 2, 3], [4, 5, 6]))
    >>> df = rdd.toDF(["type", "count"])
    >>> write_to_csv(df, 0, fn="test.csv")
    >>> os.remove("test.csv")
    """
    batchdf.toPandas().to_csv(fn, index=False)
    batchdf.unpersist()


def init_sparksession(
    name: str, shuffle_partitions: int = None, tz=None, log_level: str = "WARN"
) -> SparkSession:
    """Initialise SparkSession, the level of log for Spark and some configuration parameters

    Parameters
    ----------
    name: str
        Name for the Spark Application.
    shuffle_partitions: int, optional
        Number of partition to use when shuffling data.
        Typically better to keep the size of shuffles small.
        Default is None.
    tz: str, optional
        Timezone. Default is None.

    Returns
    -------
    spark: SparkSession
        Spark Session initialised.

    Examples
    --------
    >>> spark_tmp = init_sparksession("test")
    >>> conf = spark_tmp.sparkContext.getConf().getAll()
    """
    # Grab the running Spark Session,
    # otherwise create it.
    spark = SparkSession.builder.appName(name).getOrCreate()

    # keep the size of shuffles small
    if shuffle_partitions is not None:
        spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)

    if tz is not None:
        spark.conf.set("spark.sql.session.timeZone", tz)

    # Set spark log level to WARN
    spark.sparkContext.setLogLevel(log_level)

    return spark


def get_spark_context() -> SparkContext:
    """Return the current SparkContext.

    Raises
    ------
    RuntimeError if spark hasn't been initialized.

    Returns
    -------
    sparkContext : SparkContext instance
        The active sparkContext

    Examples
    --------
    >>> pysc = get_spark_context()
    >>> print(type(pysc))
    <class 'pyspark.context.SparkContext'>
    """
    if SparkContext._active_spark_context:
        return SparkContext._active_spark_context
    else:
        raise RuntimeError("SparkContext must be initialized")


def connect_to_kafka(
    servers: str,
    topic: str,
    startingoffsets: str = "latest",
    max_offsets_per_trigger: int = 5000,
    failondataloss: bool = False,
    kerberos: bool = False,
) -> DataFrame:
    """Initialise SparkSession, and set default Kafka parameters

    Parameters
    ----------
    servers: str
        kafka.bootstrap.servers as a comma-separated IP:PORT.
    topic: str
        Comma separated Kafka topic names.
    startingoffsets: str, optional
        From which offset you want to start pulling data. Options are:
        latest (only new data), earliest (connect from the oldest
        offset available), or a number (see Spark Kafka integration).
        Default is latest.
    max_offsets_per_trigger: int, optional
        Maximum number of offsets to fetch per trigger. Default is 5,000.
    failondataloss: bool, optional
        If True, Spark streaming job will fail if it is asking for data offsets
        that do not exist anymore in Kafka (because they have been deleted after
        exceeding a retention period for example). Default is False.
    kerberos: bool, optional
        If True, add options for a kerberized Kafka cluster. Default is False.

    Returns
    -------
    df: Streaming DataFrame
        Streaming DataFrame connected to Kafka stream

    Examples
    --------
    >>> dfstream_tmp = connect_to_kafka("localhost:29092", "ztf-stream-sim")
    >>> dfstream_tmp.isStreaming
    True
    """
    # Grab the running Spark Session
    spark = SparkSession.builder.getOrCreate()

    conf = spark.sparkContext.getConf().getAll()

    # Create a streaming DF from the incoming stream from Kafka
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", servers)
        .option("maxOffsetsPerTrigger", max_offsets_per_trigger)
    )

    if kerberos:
        df = df.option(
            "kafka.sasl.kerberos.kinit.cmd",
            'kinit -t "%{sasl.kerberos.keytab}" -k %{sasl.kerberos.principal}',
        )
        df = df.option("kafka.sasl.kerberos.service.name", "kafka")

    # Naive check for secure connection - this can be improved...
    to_secure = False
    for i in conf:
        if "-Djava.security.auth.login.config=" in i[1]:
            to_secure = True
            break
    if to_secure:
        if kerberos:
            df = df.option("kafka.security.protocol", "SASL_PLAINTEXT").option(
                "kafka.sasl.mechanism", "GSSAPI"
            )
        else:
            df = df.option("kafka.sasl.mechanism", "PLAIN").option(
                "kafka.security.protocol", "SASL_SSL"
            )

    df = (
        df.option("subscribePattern", topic)
        .option("startingOffsets", startingoffsets)
        .option("failOnDataLoss", failondataloss)
        .load()
    )

    return df


def connect_to_raw_database(basepath: str, path: str, latestfirst: bool) -> DataFrame:
    """Initialise SparkSession, and connect to the raw database (Parquet)

    Parameters
    ----------
    basepath: str
        The base path that partition discovery should start with.
    path: str
        The path to the data (typically as basepath with a glob at the end).
    latestfirst: bool
        whether to process the latest new files first,
        useful when there is a large backlog of files

    Returns
    -------
    df: Streaming DataFrame
        Streaming DataFrame connected to the database

    Examples
    --------
    >>> dfstream_tmp = connect_to_raw_database(
    ...   "online/raw/20200101", "online/raw/20200101", True)
    >>> dfstream_tmp.isStreaming
    True
    """
    # Grab the running Spark Session
    spark = SparkSession.builder.getOrCreate()

    wait_sec = 5
    while not path_exist(basepath):
        _LOG.info("Waiting for stream2raw to upload data to %s", basepath)
        time.sleep(wait_sec)
        # Sleep for longer and longer
        wait_sec = increase_wait_time(wait_sec)

    # Create a DF from the database
    # We need to wait for the schema to be available
    while True:
        try:
            userschema = spark.read.parquet(basepath).schema
        except Exception as e:  # noqa: PERF203
            _LOG.error("Error while reading %s, %s", basepath, e)
            time.sleep(wait_sec)
            wait_sec = increase_wait_time(wait_sec)
            continue
        else:
            break

    df = (
        spark.readStream.format("parquet")
        .schema(userschema)
        .option("basePath", basepath)
        .option("path", path)
        .option("latestFirst", latestfirst)
        .load()
    )

    return df


def increase_wait_time(wait_sec: int) -> int:
    """Increase the waiting time between two checks by 20%

    Parameters
    ----------
    wait_sec : int
        Current waiting time in seconds

    Returns
    -------
    int
        New waiting time in seconds, maximum 60 seconds
    """
    if wait_sec < 60:
        wait_sec *= 1.2
    return wait_sec


def path_exist(path: str) -> bool:
    """Check if a path exists on Spark shared filesystem (HDFS or S3)

    Parameters
    ----------
    path : str
        Path to check

    Returns
    -------
    bool
        True if the path exists, False otherwise
    """
    spark = SparkSession.builder.getOrCreate()

    jvm = spark._jvm
    jsc = spark._jsc

    conf = jsc.hadoopConfiguration()
    uri = jvm.java.net.URI(path)

    fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)

    path_glob = jvm.org.apache.hadoop.fs.Path(os.path.join(path, "*.parquet"))
    status_list = fs.globStatus(path_glob)
    if len(list(status_list)) > 0:
        return True
    else:
        return False


def load_parquet_files(path: str) -> DataFrame:
    """Initialise SparkSession, and load parquet files with Spark

    Unlike connect_to_raw_database, you get a standard DataFrame, and
    not a Streaming DataFrame.

    Parameters
    ----------
    path: str
        The path to the data

    Returns
    -------
    df: DataFrame
        Spark SQL DataFrame

    Examples
    --------
    >>> df = load_parquet_files(ztf_alert_sample)
    """
    # Grab the running Spark Session
    spark = SparkSession.builder.getOrCreate()

    # TODO: add mergeSchema option
    df = spark.read.format("parquet").option("mergeSchema", "true").load(path)

    return df


def get_schemas_from_avro(avro_path: str) -> Tuple[StructType, dict, str]:
    """Build schemas from an avro file (DataFrame & JSON compatibility)

    Parameters
    ----------
    avro_path: str
        Path to avro file from which schema will be extracted

    Returns
    -------
    df_schema: pyspark.sql.types.StructType
        Avro DataFrame schema
    alert_schema: dict
        Schema of the alert as a dictionary (DataFrame Style)
    alert_schema_json: str
        Schema of the alert as a string (JSON style)

    Examples
    --------
    >>> df_schema, alert_schema, alert_schema_json = get_schemas_from_avro(
    ...   ztf_avro_sample)
    >>> print(type(df_schema))
    <class 'pyspark.sql.types.StructType'>

    >>> print(type(alert_schema))
    <class 'dict'>

    >>> print(type(alert_schema_json))
    <class 'str'>
    """
    # Grab the running Spark Session
    spark = SparkSession.builder.getOrCreate()

    # Get Schema of alerts
    alert_schema = readschemafromavrofile(avro_path)
    df_schema = spark.read.format("avro").load("file://" + avro_path).schema
    alert_schema_json = json.dumps(alert_schema)

    return df_schema, alert_schema, alert_schema_json


def list_hdfs_files(hdfs_path="archive/science/year=2023/month=06/day=25"):
    """List files on an HDFS folder with full path

    Parameters
    ----------
    hdfs_path: str
        Folder name on HDFS containing files

    Returns
    -------
    paths: list of str
        List of filenames with full path
    """
    spark = SparkSession.builder.getOrCreate()

    jvm = spark._jvm
    jsc = spark._jsc

    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())
    Path = jvm.org.apache.hadoop.fs.Path
    paths = [p.getPath().toString() for p in fs.listStatus(Path(hdfs_path))]
    return paths


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    globs = globals()
    root = os.environ["FINK_HOME"]
    alert_schema_path = os.environ["FINK_SCHEMA"]

    globs["ztf_alert_sample"] = os.path.join(root, "online/raw/20200101")

    globs["ztf_avro_sample"] = os.path.join(
        alert_schema_path, "ztf/template_schema_ZTF_3p3.avro"
    )

    # Run the Spark test suite
    spark_unit_tests(globs, withstreaming=True)
