# Copyright 2019-2023 AstroLab Software
# Author: Abhishek Chauhan, Julien Peloton
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
import json
import logging
import glob
import shutil
import subprocess

from fink_broker.avroUtils import readschemafromavrofile
from fink_broker.loggingUtils import get_fink_logger
from fink_broker.sparkUtils import connect_to_raw_database, init_sparksession, to_avro

from . import distributionUtils

from pyspark.sql import DataFrame
from pyspark.sql.functions import struct, lit
from pyspark.sql.avro.functions import to_avro as to_avro_native
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

_LOG = logging.getLogger(__name__)

def create_spark_session(
        global_args: dict = None, verbose: bool = False,
        withstreaming: bool = False):
    """Create a spark session used for unit tests
    """
    if global_args is None:
        global_args = globals()

    from pyspark.sql import SparkSession
    from pyspark import SparkConf

    conf = SparkConf()
    confdic = {
        "spark.python.daemon.module": "coverage_daemon"}
    conf.setMaster("local[2]")
    conf.setAppName("fink_test")
    for k, v in confdic.items():
        conf.set(key=k, value=v)
    spark = SparkSession\
        .builder\
        .appName("fink_test")\
        .config(conf=conf)\
        .getOrCreate()

    # Reduce the number of suffled partitions
    spark.conf.set("spark.sql.shuffle.partitions", 2)

    global_args["spark"] = spark


def test_save_and_load_schema() -> None:
    """ Extract AVRO schema from a static Spark DataFrame

    Parameters
    ----------
    df: Spark DataFrame
        Spark dataframe for which we want to extract the schema
    path_for_avro: str
        Temporary path on hdfs where the schema will be written

    Returns
    ----------
    schema: str
        Schema as string
    """

    spark = init_sparksession(name="pytest", shuffle_partitions=2)


    # data path
    fink_home = os.environ['FINK_HOME']
    scitmpdatapath = os.path.join(fink_home, 'utest', 'datasets', 'science')
    checkpointpath_kafka = '/tmp/kafka_checkpoint'
    night = '20190903'

    # Connect to the TMP science database
    input_sci = scitmpdatapath + "/year={}/month={}/day={}".format(
        night[0:4], night[4:6], night[6:8])
    df = connect_to_raw_database(
        input_sci,
        input_sci,
        latestfirst=False
    )

    # Drop partitioning columns
    df = df.drop('year').drop('month').drop('day')

    # Cast fields to ease the distribution
    cnames = df.columns
    cnames[cnames.index('timestamp')] = 'cast(timestamp as string) as timestamp'
    cnames[cnames.index('cutoutScience')] = 'struct(cutoutScience.*) as cutoutScience'
    cnames[cnames.index('cutoutTemplate')] = 'struct(cutoutTemplate.*) as cutoutTemplate'
    cnames[cnames.index('cutoutDifference')] = 'struct(cutoutDifference.*) as cutoutDifference'
    cnames[cnames.index('prv_candidates')] = 'explode(array(prv_candidates)) as prv_candidates'
    cnames[cnames.index('candidate')] = 'struct(candidate.*) as candidate'
    cnames[cnames.index('lc_features_g')] = 'struct(lc_features_g.*) as lc_features_g'
    cnames[cnames.index('lc_features_r')] = 'struct(lc_features_r.*) as lc_features_r'

    # Extract schema
    df_schema = spark.read.format('parquet').load(input_sci)
    df_schema = df_schema.selectExpr(cnames)

    path_for_avro = os.path.join('/tmp',f'schema_{night}.avro')
    if os.path.isdir(path_for_avro):
        shutil.rmtree(path_for_avro)
    schema = distributionUtils.save_and_load_schema(df_schema, path_for_avro)
    # avroSchema = spark._jvm.org.apache.spark.sql.avro.SchemaConverters.toAvroType(df_schema.coalesce(1).limit(1).schema, False, "record", "dede")
    # print("XXXXXXXXXXXXXXXXXXXXXX " + avroSchema)
    _LOG.info(schema)