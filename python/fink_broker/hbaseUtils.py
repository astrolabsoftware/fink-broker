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
from pyspark.sql import DataFrame
from pyspark.mllib.common import _java2py

import os

from fink_broker.sparkUtils import get_spark_context
from fink_broker.tester import spark_unit_tests

def flatten_ztf_dataframe(df: DataFrame) -> DataFrame:
    """Flatten a nested DataFrame containing ZTF alert data.

    The input DataFrame is supposed to have the alert data (as was sent by
    the alert system) plus a column containing the timestamp from Kafka.

    Typically, from the raw database, you would do:

    df_ok = df_raw.select("decoded.*", "timestamp")
    df_ok.show(0)

    +---------+---------+--------+------+---------+
    |schemavsn|publisher|objectId|candid|candidate|
    +---------+---------+--------+------+---------+

    +--------------+-------------+--------------+----------------+---------+
    |prv_candidates|cutoutScience|cutoutTemplate|cutoutDifference|timestamp|
    +--------------+-------------+--------------+----------------+---------+

    Parameters
    ----------
    df : DataFrame
        Input DataFrame containing ZTF alert data.

    Returns
    -------
    DataFrame
        Flatten input DataFrame.

    Examples
    --------
    # Read alert from the raw database
    >>> df_raw = spark.read.format("parquet").load(ztf_alert_sample_rawdatabase)

    # Select alert data and Kafka publication timestamp
    >>> df_ok = df_raw.select("decoded.*", "timestamp")

    # Flatten the DataFrame
    >>> df_flat = flatten_ztf_dataframe(df_ok)
    """
    # Flatten the "struct" columns
    struct_cols = [
        "candidate", "cutoutScience", "cutoutTemplate", "cutoutDifference"]

    for column in struct_cols:
        df = flattenstruct(df, column)

    # Explode the "array" columns
    array_cols = ["prv_candidates"]
    for column in array_cols:
        df = explodearrayofstruct(df, column)

    return df

def construct_hbase_catalog_from_flatten_schema(
        schema: dict, catalogname: str, rowkey: str) -> str:
    """ Convert a flatten DataFrame schema into a HBase catalog.
    See flatten_ztf_dataframe for more information.

    From
    {'name': 'schemavsn', 'type': 'string', 'nullable': True, 'metadata': {}}

    To
    'schemavsn': {'cf': 'i', 'col': 'schemavsn', 'type': 'string'},

    Parameters
    ----------
    schema : dict
        Schema of the flatten DataFrame.
    catalogname : str
        Name of the HBase catalog.
    rowkey : str
        Name of the rowkey in the HBase catalog.

    Returns
    ----------
    catalog : str
        Catalog for HBase.

    Examples
    --------
    # Read alert from the raw database
    >>> df_raw = spark.read.format("parquet").load(ztf_alert_sample_rawdatabase)

    # Select alert data and Kafka publication timestamp
    >>> df_ok = df_raw.select("decoded.*", "timestamp")

    # Flatten the DataFrame
    >>> df_flat = flatten_ztf_dataframe(df_ok)

    >>> catalog = construct_hbase_catalog_from_flatten_schema(
    ...     df_flat.schema, "toto", "timestamp")
    """
    schema_columns = schema.jsonValue()["fields"]

    catalog = ''.join("""
    {{
        'table': {{
            'namespace': 'default',
            'name': '{}'
        }},
        'rowkey': '{}',
        'columns': {{
    """).format(catalogname, rowkey)

    for column in schema_columns:
        # Deal with array
        if type(column["type"]) == dict:
            column["type"] = column["type"]["type"]

        if column["name"] == rowkey:
            catalog += """
            '{}': {{'cf': 'rowkey', 'col': '{}', 'type': '{}'}},
            """.format(column["name"], column["name"], column["type"])
        else:
            catalog += """
            '{}': {{'cf': 'i', 'col': '{}', 'type': '{}'}},
            """.format(column["name"], column["name"], column["type"])
    catalog += """
        }
    }
    """

    return catalog.replace("\'", "\"")

def flattenstruct(df: DataFrame, columnname: str) -> DataFrame:
    """ From a nested column (struct of primitives),
    create one column per struct element.

    The routine accesses the JVM under the hood, and calls the
    Scala routine flattenStruct. Make sure you have the fink_broker jar
    in your classpath.

    Example:
    |-- candidate: struct (nullable = true)
    |    |-- jd: double (nullable = true)
    |    |-- fid: integer (nullable = true)

    Would become:
    |-- candidate_jd: double (nullable = true)
    |-- candidate_fid: integer (nullable = true)

    Parameters
    ----------
    df : DataFrame
        Nested Spark DataFrame
    columnname : str
        The name of the column to flatten.

    Returns
    -------
    DataFrame
        Spark DataFrame with new columns from the input column.

    Examples
    -------
    >>> df = spark.read.format("avro").load(ztf_alert_sample)

    # Candidate is nested
    >>> s = df.schema
    >>> typeOf = {i.name: i.dataType.typeName() for  i in s.fields}
    >>> typeOf['candidate'] == 'struct'
    True

    # Flatten it
    >>> df_flat = flattenstruct(df, "candidate")
    >>> "candidate_ra" in df_flat.schema.fieldNames()
    True

    # Each new column contains array element
    >>> s_flat = df_flat.schema
    >>> typeOf = {i.name: i.dataType.typeName() for  i in s_flat.fields}
    >>> typeOf['candidate_ra'] == 'double'
    True
    """
    sc = get_spark_context()
    obj = sc._jvm.com.astrolabsoftware.fink_broker.catalogUtils
    _df = obj.flattenStruct(df._jdf, columnname)
    df_flatten = _java2py(sc, _df)
    return df_flatten

def explodearrayofstruct(df: DataFrame, columnname: str) -> DataFrame:
    """From a nested column (array of struct),
    create one column per array element.

    The routine accesses the JVM under the hood, and calls the
    Scala routine explodeArrayOfStruct. Make sure you have the fink_broker jar
    in your classpath.

    Example:
    |    |-- prv_candidates: array (nullable = true)
    |    |    |-- element: struct (containsNull = true)
    |    |    |    |-- jd: double (nullable = true)
    |    |    |    |-- fid: integer (nullable = true)

    Would become:
    |-- prv_candidates_jd: array (nullable = true)
    |    |-- element: double (containsNull = true)
    |-- prv_candidates_fid: array (nullable = true)
    |    |-- element: integer (containsNull = true)

    Parameters
    ----------
    df : DataFrame
        Input nested Spark DataFrame
    columnname : str
        The name of the column to explode

    Returns
    -------
    DataFrame
        Spark DataFrame with new columns from the input column.

    Examples
    -------
    >>> df = spark.read.format("avro").load(ztf_alert_sample)

    # Candidate is nested
    >>> s = df.schema
    >>> typeOf = {i.name: i.dataType.typeName() for  i in s.fields}
    >>> typeOf['prv_candidates'] == 'array'
    True

    # Flatten it
    >>> df_flat = explodearrayofstruct(df, "prv_candidates")
    >>> "prv_candidates_ra" in df_flat.schema.fieldNames()
    True

    # Each new column contains array element
    >>> s_flat = df_flat.schema
    >>> typeOf = {i.name: i.dataType.typeName() for  i in s_flat.fields}
    >>> typeOf['prv_candidates_ra'] == 'array'
    True
    """
    sc = get_spark_context()
    obj = sc._jvm.com.astrolabsoftware.fink_broker.catalogUtils
    _df = obj.explodeArrayOfStruct(df._jdf, columnname)
    df_flatten = _java2py(sc, _df)
    return df_flatten


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    globs = globals()
    root = os.environ['FINK_HOME']
    globs["ztf_alert_sample"] = os.path.join(
        root, "schemas/template_schema_ZTF.avro")

    globs["ztf_alert_sample_rawdatabase"] = os.path.join(
        root, "schemas/template_schema_ZTF_rawdatabase.parquet")

    # Run the Spark test suite
    spark_unit_tests(globs, withstreaming=False)
