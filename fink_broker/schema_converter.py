# Copyright 2023 AstroLab Software
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

"""
Convert a Spark dataframe schema to an Avro schema
"""

import json
from typing import Any, Dict

from pyspark.sql.types import StructType

def _is_nullable(field: Dict[Any, Any], avro_type: Any) -> Any:
    avro_type_out: Any
    if field['nullable']:
        avro_type_out = [avro_type, "null"]
    elif not field['nullable']:
        avro_type_out = avro_type
    else:
        raise ValueError("Unknown value for 'nullable' key in field: ", field)
    return avro_type_out

def _parse_array(data: Dict[Any, Any], name: str) -> Dict[Any, Any]:
    out: Dict[Any, Any] = dict()
    out['type'] = "array"
    if data['elementType']['type'] == 'struct':
        items = _parse_struct(data['elementType'], name)
        if data['containsNull']:
            out['items'] = [items, "null"]
        else:
            out['items'] = items
    else:
        raise ValueError("Unknown type in parse_array()")
    return out

def _parse_map(data: Dict[Any, Any]) -> Dict[Any, Any]:
    out: Dict[Any, Any] = dict()
    out['type'] = "map"
    values = data['valueType']
    out['values'] = "map"
    if data['valueContainsNull']:
        out['values'] = [values, "null"]
    else:
        out['values'] = values
    return out

def _parse_struct(data: Dict[Any, Any], name: str = "") -> Dict[Any, Any]:
    """Convert struct below
    {
      "fields":[
      {
          SparkFieldElem1
      },
      {
          SparkFieldElem2
      },
      ...
      ]
      "type":"struct"
    }

    to

    {
      "type": "record",
      "name": "topLevelRecord",
      "fields": [
      {
          AvroFieldElem1
      },
      {
          AvroFieldElem2
      },
      ...
      ]
    }

    Parameters
    ----------
    data : Dict[Any, Any]
        Spark dataframe json schema
            {
            "name": "myname"
            "fields":[
                {
                    SparkFieldElem1
                },
                {
                    SparkFieldElem2
                },
                ...
            ]
            "type":"struct"
            }

    Returns
    -------
    Dict[Any, Any]
        Avro schema
        {
        "type": "record",
        "name": "topLevelRecord.myname",
        "fields": [
            {
                AvroFieldElem1
            },
            {
                AvroFieldElem2
            },
            ...
        ]
        }

    Raises
    ------
    ValueError
        In case an unknown entity is found in Spark Dataframe schema
    """
    avroRecord: Dict[Any, Any] = dict()
    avroRecord['type'] = "record"
    if name:
        avroRecord['name'] = "topLevelRecord." + name
    else:
        avroRecord['name'] = "topLevelRecord"
    if data['type'] not in ['struct']:
        raise ValueError("Expected ['struct'] type, is ", data['type'])
    avroRecord['fields'] = []
    avro_type: Any
    for field in data['fields']:
        outField: Dict[Any, Any] = dict()
        outField['name'] = field['name']
        if isinstance(field['type'], str):
            if field['type'] == "integer":
                avro_type = 'int'
            elif field['type'] == "binary":
                avro_type = 'bytes'
            else:
                avro_type = field['type']
            outField['type'] = _is_nullable(field, avro_type)
            avroRecord['fields'].append(outField)
        elif 'type' in field['type']:
            subData = field['type']
            if subData['type'] == 'struct':
                outField['type'] = _parse_struct(subData, field['name'])
            elif subData['type'] == 'array':
                avro_type = _parse_array(subData, field['name'])
                outField['type'] = _is_nullable(field, avro_type)
            elif subData['type'] == 'map':
                avro_type = _parse_map(subData)
                outField['type'] = _is_nullable(field, avro_type)
            else:
                raise ValueError("Unknown type", subData['type'])
            avroRecord['fields'].append(outField)
    return avroRecord

def to_avro(spark_schema: StructType) -> str:
    """Convert a Spark dataframe schema to an Avro schema

    Parameters
    ----------
    spark_schema : StructType
        Spark dataframe schema
    Returns
    -------
    str
        String containing avro schema, in json format
    """
    json_avro = _parse_struct(spark_schema.jsonValue())
    return json.dumps(json_avro, indent=2)
