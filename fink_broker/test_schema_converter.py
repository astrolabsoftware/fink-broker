# Copyright 2019-2023 AstroLab Software
# Author: Fabrice Jammes
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
import tempfile

from pyspark.sql import DataFrame

from .avroUtils import readschemafromavrofile
from .schema_converter import to_avro
from .sparkUtils import connect_to_raw_database, init_sparksession

_LOG = logging.getLogger(__name__)

def _save_and_load_schema(df: DataFrame, path_for_avro: str) -> str:
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
    # save schema
    df.coalesce(1).limit(1).write.format("avro").save(path_for_avro)

    # Read the avro schema from .avro file
    avro_file = glob.glob(path_for_avro + "/part*")[0]
    avro_schema = readschemafromavrofile(avro_file)

    # Write the schema to a file for decoding Kafka messages
    pre, _ = os.path.splitext(path_for_avro)
    ref_avro_schema = pre + '.avsc'
    with open(ref_avro_schema, 'w') as f:
        json.dump(avro_schema, f, indent=2)

    # reload the schema
    with open(ref_avro_schema, 'r') as f:
        schema = f.read()

    return schema

def test_to_avro() -> None:
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
    datapath = os.path.join(fink_home, "utest", "datasets")
    night = "20190903"

    # Connect to the TMP science database
    input_sci = os.path.join(datapath,
                             "science",
                             f"year={night[0:4]}/month={night[4:6]}/day={night[6:8]}")
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

    avro_schema_fname = f'schema_{night}'
    with tempfile.TemporaryDirectory() as tmpdir:
        path_for_avro = os.path.join(tmpdir, avro_schema_fname + ".avro")
        dumped_json_schema = _save_and_load_schema(df_schema, path_for_avro)

    schema = df_schema.coalesce(1).limit(1).schema
    json_avro = to_avro(schema)

    assert json_avro == dumped_json_schema, "Avro schema is not the expected one"

    ref_avro_schema = os.path.join(datapath, "schemas", avro_schema_fname + ".avsc")
    with open(ref_avro_schema, 'r') as file:
        data = file.read()
    assert json_avro == data, "Avro schema is not the expected one"
