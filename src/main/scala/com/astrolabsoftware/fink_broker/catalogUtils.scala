/*
 * Copyright 2019 AstroLab Software
 * Author: Julien Peloton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.astrolabsoftware.fink_broker

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column

object catalogUtils {

  /**
    * Mapping between Spark and Scala types. It has two purposes:
    * 1) fill null values
    * 2) check that a resolver exists for the input datatype.
    */
  val fallbackValues: Map[DataType, Column] = Map(
    StringType -> lit(""),
    LongType -> lit(0L),
    DoubleType -> lit(0d),
    FloatType -> lit(0f),
    IntegerType -> lit(0),
    BooleanType -> lit(true),
    BinaryType -> lit(Array[Byte]())
  )

  /**
    * From a nested column (struct of primitives), create one column per struct element.
    *
    * Example:
    * |-- candidate: struct (nullable = true)
    * |    |-- jd: double (nullable = true)
    * |    |-- fid: integer (nullable = true)
    *
    * Would become:
    * |-- candidate_jd: double (nullable = true)
    * |-- candidate_fid: integer (nullable = true)
    *
    * @param df : Nested Spark DataFrame
    * @param columnName : The name of the column to flatten.
    * @return Flatten DataFrame
    */
  def flattenStruct(df: DataFrame, columnName: String): DataFrame = {
    df.select(s"$columnName.*").schema.fields.foldLeft[DataFrame](df)( (d, structfield) => {
      val name = structfield.name
      val dataType = structfield.dataType
      val colName = s"$columnName.$name"
      val flattenedName = s"${columnName.replace('.','_')}_$name"
      if (fallbackValues.contains(dataType)) {
        d.withColumn(
          flattenedName,
          when(
            isnull(col(colName)),
            fallbackValues(dataType)
          ).otherwise(col(colName)))
      } else if (dataType.typeName == "struct") {
        // recursive decent
        flattenStruct(d, colName)
      } else {
        println(s"missing resolver for structfield type columnName=$colName dataType=${dataType.typeName}")
        d
      }
    })
  }.drop(s"$columnName")

  /**
    * From a nested column (array of struct), create one column per array element.
    *
    * Example:
    * |    |-- prv_candidates: array (nullable = true)
    * |    |    |-- element: struct (containsNull = true)
    * |    |    |    |-- jd: double (nullable = true)
    * |    |    |    |-- fid: integer (nullable = true)
    *
    * Would become:
    * |-- prv_candidates_jd: array (nullable = true)
    * |    |-- element: double (containsNull = true)
    * |-- prv_candidates_fid: array (nullable = true)
    * |    |-- element: integer (containsNull = true)
    *
    * @param df : Nested Spark DataFrame
    * @param columnName : The name of the column to flatten.
    * @return Flatten DataFrame
    */
  def explodeArrayOfStruct(df: DataFrame, columnName: String): DataFrame = {
    val colnames = df.selectExpr(s"explode($columnName) as $columnName")
      .select(s"${columnName}.*")
      .schema
      .fieldNames
      .map(name => s"CAST(${columnName}." + name + s" as string) as ${columnName}_" + name)

    // Include all other columns in the final dataframe
    val allColNames = "*" +: colnames

    df.selectExpr(allColNames: _*).drop(s"$columnName")
  }
}
