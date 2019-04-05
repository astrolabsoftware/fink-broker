/*
 * Copyright 2019 AstroLab Software
 * Author: sutugin, Julien Peloton
 * This version is heavily inspired by the work of
 * https://github.com/hortonworks-spark/shc and more specifically
 * the shc fork https://github.com/sutugin/shc
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
package HBase
import java.util.Locale

import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.execution.datasources.hbase.Logging
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Add a sink in Spark Structured Streaming to HBase table.
  *
  * @param sqlContext the SQL context
  * @param parameters HBase connector allowed parameters
  * @param partitionColumns dataframe columns
  * @param outputMode Output mode when writing stream data: complete, update, append
  */
class HBaseStreamSink(sqlContext: SQLContext,
                      parameters: Map[String, String],
                      partitionColumns: Seq[String],
                      outputMode: OutputMode)
    extends Sink
    with Logging {

  @volatile private var latestBatchId = -1L

  // This assumes the shc repo is used
  private val defaultFormat = "org.apache.spark.sql.execution.datasources.hbase"
  private val prefix = "hbase."

  // For allowed parameters, see https://github.com/hortonworks-spark/shc
  private val specifiedHBaseParams = parameters
    .keySet
    .filter(_.toLowerCase(Locale.ROOT).startsWith(prefix))
    .map { k => k.drop(prefix.length).toString -> parameters(k) }
    .toMap

  /**
    * This routine takes a DataFrame as input, and insert rows in the HBase table.
    * I will skip batchId already committed.
    *
    * @param batchId Spark Structured Streaming batch index.
    * @param data Input DataFrame
    */
  override def addBatch(batchId: Long, data: DataFrame): Unit = synchronized {
    if (batchId <= latestBatchId) {
      logInfo(s"Skipping already committed batch $batchId")
    } else {
      // use a local variable to make sure the map closure doesn't capture the whole DataFrame
      val schema = data.schema
      val res = data.queryExecution.toRdd.mapPartitions { rows =>
        val converter = CatalystTypeConverters.createToScalaConverter(schema)
        rows.map(converter(_).asInstanceOf[Row])
      }

      val df = sqlContext.sparkSession.createDataFrame(res, schema)
      df.write
        .options(specifiedHBaseParams)
        .format(defaultFormat)
        .save()
    }
  }
}

/**
  * Add a sink in Spark Structured Streaming to HBase table.
  *
  * For options, see https://github.com/hortonworks-spark/shc
  * {{{
  *   inputDF.
  *    writeStream.
  *    format("hbase").
  *    option("checkpointLocation", checkPointProdPath).
  *    option("hbase.schema_array", schema_array).
  *    option("hbase.schema_record", schema_record).
  *    option("hbase.catalog", catalog).
  *    outputMode(OutputMode.Update()).
  *    trigger(Trigger.ProcessingTime(30.seconds)).
  *    start
  * }}}
  */
class HBaseStreamSinkProvider
    extends StreamSinkProvider
    with DataSourceRegister {
  def createSink(sqlContext: SQLContext,
                 parameters: Map[String, String],
                 partitionColumns: Seq[String],
                 outputMode: OutputMode): Sink = {
    new HBaseStreamSink(sqlContext, parameters, partitionColumns, outputMode)
  }

  def shortName(): String = "hbase"
}
