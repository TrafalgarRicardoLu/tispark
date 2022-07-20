/*
 * Copyright 2021 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.v2.sink

import com.pingcap.tikv.meta.{TiDBInfo, TiTableInfo, TiTimestamp}
import com.pingcap.tikv.{StoreVersion, TiConfiguration, TiSession}
import com.pingcap.tispark.TiTableReference
import com.pingcap.tispark.auth.TiAuthorization
import com.pingcap.tispark.utils.TiUtil
import com.pingcap.tispark.write.{
  SerializableKey,
  TiBatchWrite,
  TiBatchWriteV2,
  TiDBOptions,
  TiDBWriter
}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{
  DataFrame,
  Row,
  SQLContext,
  SaveMode,
  SparkSession,
  TiContext,
  TiExtensions
}
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType

import collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * Use V1WriteBuilder before turn to v2
 */
case class TiDBBatchWrite(
    logicalInfo: LogicalWriteInfo,
    tiDBOptions: TiDBOptions,
    ticonf: TiConfiguration,
    spark: SparkSession,
    @transient tiContext: TiContext)
    extends BatchWrite {
  var schema: StructType = _
  var tiConf: TiConfiguration = _
  var tiSession: TiSession = _
  var startTs: ListBuffer[TiTimestamp] = ListBuffer[TiTimestamp]()
  var tiTableRef: TiTableReference = _
  var dbInfo: TiDBInfo = _
  var tiTableInfo: TiTableInfo = _
  var tikvSupportUpdateTTL: Boolean = _
  var isTiDBV4: Boolean = _

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    schema = logicalInfo.schema()
    tiConf = mergeSparkConfWithDataSourceConf(tiContext.conf, tiDBOptions)
    tiSession = tiContext.tiSession
    for (i <- 1 to info.numPartitions()) {
      startTs.append(tiSession.getTimestamp)
    }
    tiTableRef = tiDBOptions.getTiTableRef(ticonf)
    dbInfo = tiSession.getCatalog.getDatabase(tiTableRef.databaseName)
    tiTableInfo = tiSession.getCatalog.getTable(tiTableRef.databaseName, tiTableRef.tableName)
    tikvSupportUpdateTTL = StoreVersion.minTiKVVersion("3.0.5", tiSession.getPDClient)
    isTiDBV4 = StoreVersion.minTiKVVersion("4.0.0", tiSession.getPDClient)

    if (TiAuthorization.enableAuth) {
      val tiAuthorization = tiContext.tiAuthorization
      if (tiDBOptions.replace) {
        TiAuthorization.authorizeForInsert(tiTableInfo.getName, dbInfo.getName, tiAuthorization)
        TiAuthorization.authorizeForDelete(tiTableInfo.getName, dbInfo.getName, tiAuthorization)
      } else {
        TiAuthorization.authorizeForInsert(tiTableInfo.getName, dbInfo.getName, tiAuthorization)
      }
    }

    TiDBDataWriterFactory(
      schema,
      tiDBOptions,
      tiConf,
      startTs,
      dbInfo,
      tiTableInfo,
      tikvSupportUpdateTTL,
      isTiDBV4)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val data = messages.map(message => message.asInstanceOf[WriteSucceeded])
    data.foreach(record => {
      if (record.primaryKey != null) {
        TiBatchWriteV2.commit(
          record.primaryKey,
          schema,
          tiDBOptions,
          ticonf,
          startTs.apply(record.id.toInt),
          dbInfo,
          tiTableInfo,
          tikvSupportUpdateTTL,
          isTiDBV4)
      }
    })
//    val allData = ListBuffer[Row]()
//    data.foreach { record =>
//      if (record.rows.nonEmpty) {
//        record.rows.foreach(row => allData.append(row))
//      }
//    }
//
//    val df = spark.createDataFrame(allData.toList.asJava, logicalInfo.schema())
//    TiDBWriter.write(df, spark.sqlContext, SaveMode.Append, tiDBOptions)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}

  private def mergeSparkConfWithDataSourceConf(
      conf: SparkConf,
      options: TiDBOptions): TiConfiguration = {
    val clonedConf = conf.clone()
    // priority: data source config > spark config
    clonedConf.setAll(options.parameters)
    TiUtil.sparkConfToTiConf(clonedConf, Option.empty)
  }
}
