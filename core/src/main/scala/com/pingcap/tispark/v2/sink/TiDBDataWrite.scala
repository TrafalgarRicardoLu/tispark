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

import com.pingcap.tikv.exception.TiBatchWriteException
import com.pingcap.tikv.meta.{TiDBInfo, TiTableInfo, TiTimestamp}
import com.pingcap.tikv.{StoreVersion, TTLManager, TiConfiguration, TiDBJDBCClient, TiSession}
import com.pingcap.tispark.write.{SerializableKey, TiBatchWriteV2, TiDBOptions}
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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ListBuffer

/**
 * Use V1WriteBuilder before turn to v2
 */
case class TiDBDataWrite(
    partitionId: Int,
    taskId: Long,
    schema: StructType,
    options: TiDBOptions,
    ticonf: TiConfiguration,
    startTs: TiTimestamp,
    dbInfo: TiDBInfo,
    tiTableInfo: TiTableInfo,
    supportUpdateTTL: Boolean,
    isTiDBv4: Boolean)
    extends DataWriter[InternalRow] {
  var rows: ListBuffer[Row] = ListBuffer()
  override def write(record: InternalRow): Unit = {
    rows.append(Row.fromSeq(record.toSeq(schema)))
    println(
      "partition " + partitionId + " task " + taskId + " " + Row.fromSeq(record.toSeq(schema)))
  }

  override def commit(): WriterCommitMessage = {
//    WriteSucceeded(null, -1, rows)
    if (rows.isEmpty) {
      WriteSucceeded(null, -1, null)
    } else {
      val primaryKey = TiBatchWriteV2.preWrite(
        rows,
        schema,
        options,
        ticonf,
        startTs,
        dbInfo,
        tiTableInfo,
        supportUpdateTTL,
        isTiDBv4)
      WriteSucceeded(primaryKey, partitionId, null)
    }
  }

  override def abort(): Unit = {}

  override def close(): Unit = {}
}

case class WriteSucceeded(primaryKey: SerializableKey, id: Long, rows: ListBuffer[Row])
    extends WriterCommitMessage {}
