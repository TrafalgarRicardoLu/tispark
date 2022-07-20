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

import com.pingcap.tikv.TiConfiguration
import com.pingcap.tispark.write.TiDBOptions
import org.apache.spark.sql.{DistributedWrite, TiContext}
import org.apache.spark.sql.connector.write.{BatchWrite, LogicalWriteInfo, Write, WriteBuilder}
import org.apache.spark.sql.types.StructType
case class TiDBWriteBuilder(
    info: LogicalWriteInfo,
    tiDBOptions: TiDBOptions,
    ticontext: TiContext,
    schema: StructType,
    ticonf: TiConfiguration)
    extends WriteBuilder {
  override def build(): Write = {
//    val tiTableRef = tiDBOptions.getTiTableRef(ticonf)
//    val dbInfo = ticontext.tiSession.getCatalog.getDatabase(tiTableRef.databaseName)
//    val tiTableInfo = ticontext.tiSession.getCatalog.getTable(tiTableRef.databaseName, tiTableRef.tableName)
//
//    var colName =""
//    if(tiTableInfo.hasPrimaryKey){
//      colName=tiTableInfo.getPrimaryKey.getName
//    }else if(tiTableInfo.isPkHandle){
//      colName=tiTableInfo.getPKIsHandleColumn.getName
//    }else{
//      val uniqueIndices = tiTableInfo.getIndices
//
//    }
    val numPartition = ticonf.getPartitionPerSplit
    new DistributedWrite(numPartition) {
      override def toBatch: BatchWrite = {
        TiDBBatchWrite(info, tiDBOptions, ticonf, ticontext.sparkSession, ticontext)
      }
    }
  }
}
//  override def build(): V1Write =
//    new V1Write {
//      override def toInsertableRelation: InsertableRelation = {
//        new InsertableRelation {
//          override def insert(data: DataFrame, overwrite: Boolean): Unit = {
//            val df = sqlContext.sparkSession.createDataFrame(data.toJavaRDD, schema)
//            df.write
//              .format("tidb")
//              .options(tiDBOptions.parameters)
//              .option("database", tiDBOptions.database)
//              .option("table", tiDBOptions.table)
//              .mode("append")
//              .save()
//          }
//        }
//      }
//    }
