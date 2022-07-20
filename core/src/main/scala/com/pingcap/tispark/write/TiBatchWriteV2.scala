/*
 * Copyright 2019 PingCAP, Inc.
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

package com.pingcap.tispark.write

import com.pingcap.tikv._
import com.pingcap.tikv.exception.TiBatchWriteException
import com.pingcap.tikv.meta.{TiDBInfo, TiTableInfo, TiTimestamp}
import com.pingcap.tispark.TiDBUtils
import com.pingcap.tispark.auth.TiAuthorization
import com.pingcap.tispark.utils.{SchemaUpdateTime, TiUtil, TwoPhaseCommitHepler}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession, TiContext, TiExtensions}
import org.slf4j.LoggerFactory

import javax.jdo.annotations.PrimaryKey
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object TiBatchWriteV2 {
  type SparkRow = org.apache.spark.sql.Row
  type TiRow = com.pingcap.tikv.row.Row
  type TiDataType = com.pingcap.tikv.types.DataType
  // Milliseconds
  private val MIN_DELAY_CLEAN_TABLE_LOCK = 60000
  private val DELAY_CLEAN_TABLE_LOCK_AND_COMMIT_BACKOFF_DELTA = 30000
  private val PRIMARY_KEY_COMMIT_BACKOFF =
    MIN_DELAY_CLEAN_TABLE_LOCK - DELAY_CLEAN_TABLE_LOCK_AND_COMMIT_BACKOFF_DELTA

//  @throws(classOf[NoSuchTableException])
//  @throws(classOf[TiBatchWriteException])
//  def write(
//             rows: ListBuffer[Row],
//             tiContext: TiContext,
//             options: TiDBOptions,
//             schema: StructType): Unit = {
//    val dataToWrite = Map(DBTable(options.database, options.table) -> rows)
//    new TiBatchWriteV2(dataToWrite, tiContext, options, schema).write()
//  }
//
//  @throws(classOf[NoSuchTableException])
//  @throws(classOf[TiBatchWriteException])
//  def write(
//      dataToWrite: Map[DBTable, ListBuffer[Row]],
//      sparkSession: SparkSession,
//      parameters: Map[String, String],
//      schema: StructType): Unit = {
//    TiExtensions.getTiContext(sparkSession) match {
//      case Some(tiContext) =>
//        val tiDBOptions = new TiDBOptions(
//          parameters ++ Map(TiDBOptions.TIDB_MULTI_TABLES -> "true"))
//        tiDBOptions.checkWriteRequired()
//        new TiBatchWriteV2(dataToWrite, tiContext, tiDBOptions, schema).write()
//      case None =>
//        throw new TiBatchWriteException("TiExtensions is disable!")
//    }
//  }

  @throws(classOf[NoSuchTableException])
  @throws(classOf[TiBatchWriteException])
  def preWrite(
      rows: ListBuffer[Row],
      schema: StructType,
      options: TiDBOptions,
      ticonf: TiConfiguration,
      startTs: TiTimestamp,
      dbInfo: TiDBInfo,
      tiTableInfo: TiTableInfo,
      supportUpdateTTL: Boolean,
      isTiDBv4: Boolean): SerializableKey = {
    val dataToWrite = Map(DBTable(options.database, options.table) -> rows)
    new TiBatchWriteV2(
      dataToWrite,
      schema,
      options,
      ticonf,
      startTs,
      dbInfo,
      tiTableInfo,
      supportUpdateTTL,
      isTiDBv4).preWrite();
  }

  @throws(classOf[NoSuchTableException])
  @throws(classOf[TiBatchWriteException])
  def commit(
      primaryKey: SerializableKey,
      schema: StructType,
      options: TiDBOptions,
      ticonf: TiConfiguration,
      startTs: TiTimestamp,
      dbInfo: TiDBInfo,
      tiTableInfo: TiTableInfo,
      supportUpdateTTL: Boolean,
      isTiDBv4: Boolean): Unit = {
    val dataToWrite = Map(DBTable(options.database, options.table) -> null)
    new TiBatchWriteV2(
      dataToWrite,
      schema,
      options,
      ticonf,
      startTs,
      dbInfo,
      tiTableInfo,
      supportUpdateTTL,
      isTiDBv4).commit(primaryKey)
  }
}

class TiBatchWriteV2(
    @transient val dataToWrite: Map[DBTable, ListBuffer[Row]],
    schema: StructType,
    options: TiDBOptions,
    tiConf: TiConfiguration,
    startTimeStamp: TiTimestamp,
    dbInfo: TiDBInfo,
    tiTableInfo: TiTableInfo,
    supportUpdateTTL: Boolean,
    isTiDBV4: Boolean)
    extends Serializable {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  import com.pingcap.tispark.write.TiBatchWrite._

  private var useTableLock: Boolean = _
  @transient private var ttlManager: TTLManager = _
  private var isTTLUpdate: Boolean = _
  private var lockTTLSeconds: Long = _
  @transient private var tiDBJDBCClient: TiDBJDBCClient = _
  @transient private var tiBatchWriteTables: List[TiBatchWriteTableV2] = _
  @transient private var startMS: Long = _
  private var startTs: Long = _
  private var twoPhaseCommitHepler: TwoPhaseCommitHepler = _

  private def preWrite(): SerializableKey = {
    try {
      doPreWrite()
    } finally {
      close()
    }
  }

  private def commit(primaryKey: SerializableKey): Unit = {
    try {
      doCommit(primaryKey)
    } finally {
      close()
    }
  }

  private def close(): Unit = {
    try {
      if (tiBatchWriteTables != null) {
        tiBatchWriteTables.foreach(_.unlockTable())
      }
    } catch {
      case _: Throwable =>
    }

    try {
      twoPhaseCommitHepler.close()
    } catch {
      case _: Throwable =>
    }

    try {
      if (tiDBJDBCClient != null) {
        tiDBJDBCClient.close()
      }
    } catch {
      case _: Throwable =>
    }
  }

  private def doPreWrite(): SerializableKey = {
    startMS = System.currentTimeMillis()

    // check if write enable
    if (!tiConf.isWriteEnable) {
      throw new TiBatchWriteException(
        "tispark batch write is disabled! set spark.tispark.write.enable to enable.")
    }

    // initialize
    isTTLUpdate = options.isTTLUpdate(supportUpdateTTL)
    lockTTLSeconds = options.getLockTTLSeconds(supportUpdateTTL)
    tiDBJDBCClient = new TiDBJDBCClient(TiDBUtils.createConnectionFactory(options.url)())

    // init tiBatchWriteTables
    tiBatchWriteTables = {
      dataToWrite.map {
        case (dbTable, rows) =>
          new TiBatchWriteTableV2(
            rows,
            options.setDBTable(dbTable),
            tiConf,
            tiDBJDBCClient,
            isTiDBV4,
            schema,
            dbInfo,
            tiTableInfo)
      }.toList
    }

    // check unsupported
    tiBatchWriteTables.foreach(_.checkUnsupported())

    // check empty
    var allEmpty = true
    tiBatchWriteTables.foreach { table =>
      if (!table.isDFEmpty) {
        allEmpty = false
      }
    }
    if (allEmpty) {
      logger.warn("data is empty!")
      return null
    }

    // lock table
    useTableLock = getUseTableLock
    if (useTableLock) {
      tiBatchWriteTables.foreach(_.lockTable())
    } else {
      if (!isTiDBV4) {
        if (tiConf.isWriteWithoutLockTable) {
          logger.warn("write tidb-2.x or 3.x without lock table enabled! only for test!")
        } else {
          throw new TiBatchWriteException(
            "current tidb does not support LockTable or is disabled!")
        }
      }
    }

    // check schema
    tiBatchWriteTables.foreach(_.checkColumnNumbers())

    // get timestamp as start_ts
    startTs = startTimeStamp.getVersion
    logger.info(s"startTS: $startTs")

    // pre calculate
    val shuffledRDD: ListBuffer[(SerializableKey, Array[Byte])] = {
      val rddList = tiBatchWriteTables.map(_.preCalculate(startTimeStamp))
      if (rddList.lengthCompare(1) == 0) {
        rddList.head
      } else {
        rddList.head
      }
    }
    shuffledRDD.foreach { println }

    // take one row as primary key
    val (primaryKey: SerializableKey, primaryRow: Array[Byte]) = {
      val takeOne = shuffledRDD.take(1)
      if (takeOne.isEmpty) {
        logger.warn("there is no data in source rdd")
        return null
      } else {
        takeOne.head
      }
    }
    println("primary key " + primaryKey)

    logger.info(s"primary key: $primaryKey")

    // filter primary key
    val secondaryKeysRDD = shuffledRDD.toList.filter(keyValue => !keyValue._1.equals(primaryKey))

    // for test
    if (options.sleepBeforePrewritePrimaryKey > 0) {
      logger.info(s"sleep ${options.sleepBeforePrewritePrimaryKey} ms for test")
      Thread.sleep(options.sleepBeforePrewritePrimaryKey)
    }

    twoPhaseCommitHepler = TwoPhaseCommitHepler(startTs, options)

    // driver primary pre-write
    twoPhaseCommitHepler.prewritePrimaryKeyByDriver(primaryKey, primaryRow)

    // for test
    if (options.sleepAfterPrewritePrimaryKey > 0) {
      logger.info(s"sleep ${options.sleepAfterPrewritePrimaryKey} ms for test")
      Thread.sleep(options.sleepAfterPrewritePrimaryKey)
    }

    // executors secondary pre-write
    twoPhaseCommitHepler.prewriteSecondaryKeyByExecutorsForRow(secondaryKeysRDD, primaryKey)

    println(startTs)
    primaryKey
  }

  private def doCommit(primaryKey: SerializableKey): Unit = {
    // check connection lost if using lock table
    isTTLUpdate = options.isTTLUpdate(supportUpdateTTL)
    lockTTLSeconds = options.getLockTTLSeconds(supportUpdateTTL)
    tiDBJDBCClient = new TiDBJDBCClient(TiDBUtils.createConnectionFactory(options.url)())
    startTs = startTimeStamp.getVersion
    twoPhaseCommitHepler = TwoPhaseCommitHepler(startTs, options)
    // init tiBatchWriteTables
    tiBatchWriteTables = {
      dataToWrite.map {
        case (dbTable, rows) =>
          new TiBatchWriteTableV2(
            rows,
            options.setDBTable(dbTable),
            tiConf,
            tiDBJDBCClient,
            isTiDBV4,
            schema,
            dbInfo,
            tiTableInfo)
      }.toList
    }

    checkConnectionLost()
    // checkschema if not useTableLock
    val schemaUpdateTimes = if (useTableLock) {
      Nil
    } else {
      tiBatchWriteTables.map(_.buildSchemaUpdateTime())
    }

    // driver primary commit
    twoPhaseCommitHepler.commitPrimaryKeyWithRetryByDriver(primaryKey, schemaUpdateTimes)

    // stop ttl
    twoPhaseCommitHepler.stopPrimaryKeyTTLUpdate()

    // unlock table
    tiDBJDBCClient.unlockTables()

    // update table statistics: modify_count & count
    if (options.enableUpdateTableStatistics) {
      tiBatchWriteTables.foreach(_.updateTableStatistics(startTs))
    }
  }

  private def getRegionSplitPoints(
      rdd: ListBuffer[(SerializableKey, Array[Byte])]): List[SerializableKey] = {
    val count = rdd.length

    if (count < options.regionSplitThreshold) {
      return Nil
    }

    val regionSplitPointNum = if (options.regionSplitNum > 0) {
      options.regionSplitNum
    } else {
      Math.min(
        Math.max(
          options.minRegionSplitNum,
          Math.ceil(count.toDouble / options.regionSplitKeys).toInt),
        options.maxRegionSplitNum)
    }
    logger.info(s"regionSplitPointNum=$regionSplitPointNum")

    val sampleSize = (regionSplitPointNum + 1) * options.sampleSplitFrac
    logger.info(s"sampleSize=$sampleSize")

    val sampleData = rdd.takeRight(sampleSize / count)
    logger.info(s"sampleData size=${sampleData.length}")

    val splitPointNumUsingSize = if (options.regionSplitUsingSize) {
      val avgSize = getAverageSizeInBytes(sampleData)
      logger.info(s"avgSize=$avgSize Bytes")
      if (avgSize <= options.bytesPerRegion / options.regionSplitKeys) {
        regionSplitPointNum
      } else {
        Math.min(
          Math.floor((count.toDouble / options.bytesPerRegion) * avgSize).toInt,
          sampleData.length / 10)
      }
    } else {
      regionSplitPointNum
    }
    logger.info(s"splitPointNumUsingSize=$splitPointNumUsingSize")

    val finalRegionSplitPointNum = Math.min(
      Math.max(options.minRegionSplitNum, splitPointNumUsingSize),
      options.maxRegionSplitNum)
    logger.info(s"finalRegionSplitPointNum=$finalRegionSplitPointNum")

    val sortedSampleData = sampleData
      .map(_._1)
      .sorted(new Ordering[SerializableKey] {
        override def compare(x: SerializableKey, y: SerializableKey): Int = {
          x.compareTo(y)
        }
      })
    val orderedSplitPoints = new Array[SerializableKey](finalRegionSplitPointNum)
    val step = Math.floor(sortedSampleData.length.toDouble / (finalRegionSplitPointNum + 1)).toInt
    for (i <- 0 until finalRegionSplitPointNum) {
      orderedSplitPoints(i) = sortedSampleData((i + 1) * step)
    }

    logger.info(s"orderedSplitPoints size=${orderedSplitPoints.length}")
    orderedSplitPoints.toList
  }

  private def getAverageSizeInBytes(
      keyValues: ListBuffer[(SerializableKey, Array[Byte])]): Int = {
    var avg: Double = 0
    var t: Int = 1
    keyValues.foreach { keyValue =>
      val keySize: Double = keyValue._1.bytes.length + keyValue._2.length
      avg = avg + (keySize - avg) / t
      t = t + 1
    }
    Math.ceil(avg).toInt
  }

  private def getUseTableLock: Boolean = {
    if (!options.useTableLock(isTiDBV4)) {
      false
    } else {
      if (tiDBJDBCClient.isEnableTableLock) {
        if (tiDBJDBCClient.getDelayCleanTableLock >= 60000) {
          true
        } else {
          logger.warn(
            s"table lock disabled! to enable table lock, please set tidb config: delay-clean-table-lock >= 60000")
          false
        }
      } else {
        false
      }
    }
  }

  private def checkConnectionLost(): Unit = {
    if (useTableLock) {
      if (tiDBJDBCClient.isClosed) {
        throw new TiBatchWriteException("tidb's jdbc connection is lost!")
      }
    }
  }

  private def mergeSparkConfWithDataSourceConf(
      conf: SparkConf,
      options: TiDBOptions): TiConfiguration = {
    val clonedConf = conf.clone()
    // priority: data source config > spark config
    clonedConf.setAll(options.parameters)
    TiUtil.sparkConfToTiConf(clonedConf, Option.empty)
  }
}
