/*
 * Copyright 2020 PingCAP, Inc.
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

import com.pingcap.tikv.allocator.RowIDAllocator
import com.pingcap.tikv.codec.TableCodec
import com.pingcap.tikv.exception.TiBatchWriteException
import com.pingcap.tikv.key.{Handle, IndexKey, IntHandle, RowKey}
import com.pingcap.tikv.meta._
import com.pingcap.tikv.{BytePairWrapper, TiConfiguration, TiDBJDBCClient, TiSession}
import com.pingcap.tispark.TiTableReference
import com.pingcap.tispark.auth.TiAuthorization
import com.pingcap.tispark.utils.{SchemaUpdateTime, TiUtil, WriteUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{TiContext, _}
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class TiBatchWriteTableV2(
    @transient var rows: ListBuffer[Row],
    val options: TiDBOptions,
    val tiConf: TiConfiguration,
    @transient val tiDBJDBCClient: TiDBJDBCClient,
    val isTiDBV4: Boolean,
    val schema: StructType,
    tiDBInfo: TiDBInfo,
    tiTableInfo: TiTableInfo)
    extends Serializable {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  import com.pingcap.tispark.write.TiBatchWrite._
  // only fetch row format version once for each batch write process
  private val enableNewRowFormat: Boolean =
    if (isTiDBV4) tiDBJDBCClient.getRowFormatVersion == 2 else false
  private var tiTableRef: TiTableReference = _
  private var tableColSize: Int = _
  private var colsMapInTiDB: Map[String, TiColumnInfo] = _
  private var colsInDf: List[String] = _
  private var uniqueIndices: Seq[TiIndexInfo] = _
  private var handleCol: TiColumnInfo = _
  private var tableLocked: Boolean = false
  private var autoIncProvidedID: Boolean = false
  // isCommonHandle = true => clustered index
  private var isCommonHandle: Boolean = _
  private var deltaCount: Long = 0
  private var modifyCount: Long = 0

  tiTableRef = options.getTiTableRef(tiConf)

  if (tiTableInfo == null) {
    throw new NoSuchTableException(tiTableRef.databaseName, tiTableRef.tableName)
  }

  isCommonHandle = tiTableInfo.isCommonHandle
  colsMapInTiDB = tiTableInfo.getColumns.asScala.map(col => col.getName -> col).toMap
  colsInDf = schema.fieldNames.toList
  println(colsInDf)
  uniqueIndices = tiTableInfo.getIndices.asScala.filter(index => index.isUnique)
  handleCol = tiTableInfo.getPKIsHandleColumn
  tableColSize = tiTableInfo.getColumns.size()

  def isDFEmpty: Boolean = {
    if (rows.length == 0) {
      logger.warn(s"the dataframe write to $tiTableRef is empty!")
      true
    } else {
      false
    }
  }

  def buildSchemaUpdateTime(): SchemaUpdateTime = {
    SchemaUpdateTime(
      tiTableRef.databaseName,
      tiTableRef.tableName,
      tiTableInfo.getUpdateTimestamp)
  }

  def preCalculate(startTimeStamp: TiTimestamp): ListBuffer[(SerializableKey, Array[Byte])] = {
    val count = rows.length
    logger.info(s"source data count=$count")

    // a rough estimate to deltaCount and modifyCount
    deltaCount = count
    modifyCount = count

    // auto increment
    val rdd = if (tiTableInfo.hasAutoIncrementColumn) {
      val autoIncrementColName = tiTableInfo.getAutoIncrementColInfo.getName
      val autoIncrementColIndex = colsInDf.indexOf(autoIncrementColName, 0)

      def allNullOnAutoIncrement: Boolean = {
        rows.forall(row => row.get(autoIncrementColIndex) == null)
      }

      def isProvidedID: Boolean = {
        if (tableColSize != colsInDf.length) {
          false
        } else {
          if (allNullOnAutoIncrement) {
            rows.remove(autoIncrementColIndex)
            false
          } else {
            true
          }
        }
      }

      // when auto increment column is provided but the corresponding column in df contains null,
      // we need throw exception
      if (isProvidedID) {
        autoIncProvidedID = true

        if (!options.replace) {

          val colNames = tiTableInfo.getColumns.asScala.map(col => col.getName).mkString(", ")
          throw new TiBatchWriteException(
            s"""currently user provided auto increment value is only supported in update mode!
               |please set parameter replace to true!
               |
               |colsInDf.length = ${colsInDf.length}
               |df.schema = ${schema}
               |
                $tableColSize
               s = $colNames
              |
              |tiTableInfo = $tiTableInfo
            """.stripMargin)
        }

        if (!colsInDf.contains(autoIncrementColName)) {
          throw new TiBatchWriteException(
            "Column size is matched but cannot find auto increment column by name")
        }

        val hasNullValue = !rows.forall(row => row.get(autoIncrementColIndex) != null)
        if (hasNullValue) {
          throw new TiBatchWriteException(
            "cannot allocate id on the condition of having null value and valid value on auto increment column")
        }
        rows
      } else {
        // if auto increment column is not provided, we need allocate id for it.
        // adding an auto increment column to df
        println("allocate auto col")
        schema.add(autoIncrementColName, "long")
        val rowIDAllocator = getRowIDAllocator(count)

        // update colsInDF since we  st add one column in df
        colsInDf = colsInDf.map(_.toLowerCase())
        // last one is auto increment column
        rows.zipWithIndex.map { row =>
          val rowSep = row._1.toSeq.zipWithIndex.map { data =>
            val colOffset = data._2
            if (colsMapInTiDB.contains(colsInDf(colOffset))) {
              if (colsMapInTiDB(colsInDf(colOffset)).isAutoIncrement) {
                val index = row._2 + 1
                rowIDAllocator.getAutoIncId(index)
              } else {
                data._1
              }
            }
          }
          Row
            .fromSeq(rowSep)
        }
        rows
      }
    } else {
      println("no auto col")
      rows
    }

    // spark row -> tikv row
    val tiRowRdd = rdd.map(row => WriteUtil.sparkRow2TiKVRow(row, tiTableInfo, colsInDf))
    println("tiRowRDD")
    tiRowRdd.foreach(println)
    // check value not null
    checkValueNotNull(tiRowRdd)

    // for partition table, we need calculate each row and tell which physical table
    // that row is belong to.
    // currently we only support replace and insert.
    val constraintCheckIsNeeded = isCommonHandle || handleCol != null || uniqueIndices.nonEmpty

    val keyValueRDD = if (constraintCheckIsNeeded) {
      println("constrain " + constraintCheckIsNeeded)
      val wrappedRowRdd = if (isCommonHandle || tiTableInfo.isPkHandle) {
        tiRowRdd.map { row =>
          WrappedRow(row, WriteUtil.extractHandle(row, tiTableInfo))
        }
      } else {
        val rowIDAllocator = getRowIDAllocator(count)
        tiRowRdd.zipWithIndex.map { row =>
          val index = row._2 + 1
          val rowId = rowIDAllocator.getShardRowId(index)
          WrappedRow(row._1, new IntHandle(rowId))
        }
      }
      wrappedRowRdd.foreach(println)

      val distinctWrappedRowRdd = deduplicate(wrappedRowRdd)
      println("deduplicate " + options.deduplicate)
      distinctWrappedRowRdd.foreach(println)
      if (!options.deduplicate) {
        val c1 = wrappedRowRdd.size
        val c2 = distinctWrappedRowRdd.size
        println(c1 + " " + c2)
        if (c1 != c2) {
          throw new TiBatchWriteException("duplicate unique key or primary key")
        }
      }
      var deletion = ListBuffer[WrappedRow]()
      if (options.useSnapshotBatchGet) {
        deletion = generateDataToBeRemoved(distinctWrappedRowRdd, startTimeStamp)
      }

      deletion.foreach(println)
      if (!options.replace && deletion.nonEmpty) {
        throw new TiBatchWriteException("data to be inserted has conflicts with TiKV data")
      }

      if (autoIncProvidedID) {
        if (deletion.length != count) {
          throw new TiBatchWriteException(
            "currently user provided auto increment value is only supported in update mode!")
        }
      }

      val wrappedEncodedRecordRdd = generateRecordKV(distinctWrappedRowRdd, remove = false)
      val wrappedEncodedDeleteRecordRDD = generateRecordKV(deletion, remove = true)
      val recordSet = wrappedEncodedRecordRdd.map(row => row.encodedKey).toSet
      wrappedEncodedDeleteRecordRDD.foreach { wrappedRow =>
        {
          if (!recordSet.apply(wrappedRow.encodedKey)) {
            wrappedEncodedRecordRdd.append(wrappedRow)
          }
        }
      }
      var wrappedEncodedIndexRdd =
        WriteUtil
          .generateIndexKVs(distinctWrappedRowRdd, tiTableInfo, remove = false)
          .values
          .toList
          .flatten
      val wrappedEncodedDeleteIndex =
        WriteUtil.generateIndexKVs(deletion, tiTableInfo, remove = true).values.toList.flatten
      println("delete")
      wrappedEncodedDeleteIndex.foreach(println)
      println("origin")
      wrappedEncodedIndexRdd.foreach(println)
      val indexSet = wrappedEncodedIndexRdd.map(row => row.encodedKey).toSet
      wrappedEncodedDeleteIndex.foreach { wrappedRow =>
        if (!indexSet.apply(wrappedRow.encodedKey)) {
          wrappedEncodedIndexRdd = wrappedEncodedIndexRdd :+ wrappedRow
        }
      }

      println("record")
      wrappedEncodedRecordRdd.foreach(println)
      println("index")
      wrappedEncodedIndexRdd.foreach(println)

      wrappedEncodedIndexRdd.foreach(rdd => wrappedEncodedRecordRdd.append(rdd))
      val finalans = wrappedEncodedRecordRdd.map(obj => (obj.encodedKey, obj.encodedValue))
      finalans
    } else {
      println("constrain " + constraintCheckIsNeeded)
      val rowIDAllocator = getRowIDAllocator(count)
      val wrappedRowRdd = tiRowRdd.zipWithIndex.map { row =>
        val index = row._2 + 1
        val rowId = rowIDAllocator.getShardRowId(index)
        WrappedRow(row._1, new IntHandle(rowId))
      }
      wrappedRowRdd.foreach(println)

      val wrappedEncodedRecordRdd = generateRecordKV(wrappedRowRdd, remove = false)
      val wrappedEncodedIndexRdds =
        WriteUtil.generateIndexKVs(wrappedRowRdd, tiTableInfo, remove = false).values.toList

      wrappedEncodedIndexRdds.foreach(rddList =>
        rddList.foreach(rdd => wrappedEncodedRecordRdd.append(rdd)))

      println("record")
      wrappedEncodedRecordRdd.foreach(println)
      println("index")
      wrappedEncodedIndexRdds.foreach(println)

      val finalans = wrappedEncodedRecordRdd.map(obj => (obj.encodedKey, obj.encodedValue))
      finalans
    }

    keyValueRDD
  }

  def lockTable(): Unit = {
    if (!tableLocked) {
      tiDBJDBCClient.lockTableWriteLocal(options.database, options.table)
      tableLocked = true
    } else {
      logger.warn("table already locked!")
    }
  }

  def unlockTable(): Unit = {
    if (tableLocked) {
      tiDBJDBCClient.unlockTables()
      tableLocked = false
    }
  }

  def checkUnsupported(): Unit = {
    // write to table with auto random column
    if (tiTableInfo.hasAutoRandomColumn) {
      throw new TiBatchWriteException(
        "tispark currently does not support write data to table with auto random column!")
    }

    // write to partition table
    if (tiTableInfo.isPartitionEnabled) {
      throw new TiBatchWriteException(
        "tispark currently does not support write data to partition table!")
    }

    // write to table with generated column
    if (tiTableInfo.hasGeneratedColumn) {
      throw new TiBatchWriteException(
        "tispark currently does not support write data to table with generated column!")
    }
  }

  def checkAuthorization(tiAuthorization: Option[TiAuthorization], options: TiDBOptions): Unit = {
    if (options.replace) {
      TiAuthorization.authorizeForInsert(tiTableInfo.getName, tiDBInfo.getName, tiAuthorization)
      TiAuthorization.authorizeForDelete(tiTableInfo.getName, tiDBInfo.getName, tiAuthorization)
    } else {
      TiAuthorization.authorizeForInsert(tiTableInfo.getName, tiDBInfo.getName, tiAuthorization)
    }
  }

  def checkColumnNumbers(): Unit = {
    if (!tiTableInfo.hasAutoIncrementColumn && colsInDf.lengthCompare(tableColSize) != 0) {
      throw new TiBatchWriteException(
        s"table without auto increment column, but data col size ${colsInDf.length} != table column size $tableColSize")
    }

    if (tiTableInfo.hasAutoIncrementColumn && colsInDf.lengthCompare(
        tableColSize) != 0 && colsInDf.lengthCompare(tableColSize - 1) != 0) {
      throw new TiBatchWriteException(
        s"table with auto increment column, but data col size ${colsInDf.length} != table column size $tableColSize and table column size - 1 ${tableColSize - 1} ")
    }
  }

  // update table statistics: modify_count & count
  def updateTableStatistics(startTs: Long): Unit = {
    try {
      tiDBJDBCClient.updateTableStatistics(startTs, tiTableInfo.getId, deltaCount, modifyCount)
    } catch {
      case e: Throwable => logger.warn("updateTableStatistics error!", e)
    }
  }

  private def getRowIDAllocator(step: Long): RowIDAllocator = {
    RowIDAllocator.create(
      tiDBInfo.getId,
      tiTableInfo,
      tiConf,
      tiTableInfo.isAutoIncColUnsigned,
      step)
  }

  private def isNullUniqueIndexValue(value: Array[Byte]): Boolean = {
    value.length == 1 && value(0) == '0'
  }

  private def generateDataToBeRemoved(
      rdd: ListBuffer[WrappedRow],
      startTs: TiTimestamp): ListBuffer[WrappedRow] = {
    val rowBuf = mutable.ListBuffer.empty[WrappedRow]
    val snapshot = TiSession.getInstance(tiConf).createSnapshot(startTs.getPrevious)
    rdd
      .foreach { wrappedRow =>
        {
          //  check handle key
          if (handleCol != null || isCommonHandle) {
            val oldValue = snapshot.get(buildRowKey(wrappedRow.row, wrappedRow.handle).bytes)
            if (oldValue.nonEmpty && !isNullUniqueIndexValue(oldValue)) {
              val oldRow = TableCodec.decodeRow(oldValue, wrappedRow.handle, tiTableInfo)
              rowBuf += WrappedRow(oldRow, wrappedRow.handle)
            }
          }

          uniqueIndices.foreach { index =>
            if (!isCommonHandle || !index.isPrimary) {
              val keyInfo = buildUniqueIndexKey(wrappedRow.row, wrappedRow.handle, index)
              // if handle is appended, it must not exists in old table
              if (!keyInfo._2) {
                val oldValue = snapshot.get(keyInfo._1.bytes)
                if (oldValue.nonEmpty && !isNullUniqueIndexValue(oldValue)) {
                  val oldHandle = TableCodec.decodeHandle(oldValue, isCommonHandle)
                  val oldRowValue = snapshot.get(buildRowKey(wrappedRow.row, oldHandle).bytes)
                  val oldRow = TableCodec.decodeRow(oldRowValue, oldHandle, tiTableInfo)
                  rowBuf += WrappedRow(oldRow, oldHandle)
                }
              }
            }
          }
        }
      }
    rowBuf
  }

  private def checkValueNotNull(tirow: ListBuffer[TiRow]): Unit = {
    val nullRows = tirow
      .forall(row =>
        colsMapInTiDB.exists {
          case (_, v) =>
            if (v.getType.isNotNull && row.get(v.getOffset, v.getType) == null) {
              true
            } else {
              false
            }
        })
    if (nullRows) {
      throw new TiBatchWriteException(
        s"Insert null value to not null column! rows contain illegal null values!")
    }
  }

  @throws(classOf[TiBatchWriteException])
  private def deduplicate(rdd: ListBuffer[WrappedRow]): ListBuffer[WrappedRow] = {
    //1 handle key
    val mutableRdd = rdd.clone()
    if (handleCol != null) {
      val keyRowSet = mutable.HashSet[SerializableKey]()
      rdd.foreach { wrappedRow =>
        val rowKey = buildRowKey(wrappedRow.row, wrappedRow.handle)
        if (keyRowSet.apply(rowKey)) {
          mutableRdd.remove(mutableRdd.indexOf(wrappedRow))
        } else {
          keyRowSet.add(rowKey)
        }
      }
    }
    val keyIndexSet = mutable.HashSet[SerializableKey]()
    val finalRdd = mutableRdd
    uniqueIndices.foreach { index =>
      {
        mutableRdd.foreach { wrappedRow =>
          val indexKey = buildUniqueIndexKey(wrappedRow.row, wrappedRow.handle, index)._1
          if (keyIndexSet.apply(indexKey)) {
            finalRdd.remove(finalRdd.indexOf(wrappedRow))
          } else {
            keyIndexSet.add(indexKey)
          }
        }
      }
    }
    finalRdd
  }

  @throws(classOf[TiBatchWriteException])
  private def encodeTiRow(tiRow: TiRow): Array[Byte] = {
    val colSize = tiRow.fieldCount()

    if (colSize > tableColSize) {
      throw new TiBatchWriteException(s"data col size $colSize > table column size $tableColSize")
    }

    // TODO: ddl state change
    // pending: https://internal.pingcap.net/jira/browse/TISPARK-82
    val convertedValues = new Array[AnyRef](colSize)
    for (i <- 0 until colSize) {
      // pk is handle can be skipped
      val columnInfo = tiTableInfo.getColumn(i)
      val value = tiRow.get(i, columnInfo.getType)
      convertedValues.update(i, value)
    }

    TableCodec.encodeRow(
      tiTableInfo.getColumns,
      convertedValues,
      tiTableInfo.isPkHandle,
      enableNewRowFormat)
  }

  private def buildRowKey(row: TiRow, handle: Handle): SerializableKey = {
    new SerializableKey(
      RowKey.toRowKey(WriteUtil.locatePhysicalTable(row, tiTableInfo), handle).getBytes)
  }

  private def buildUniqueIndexKey(
      row: TiRow,
      handle: Handle,
      index: TiIndexInfo): (SerializableKey, Boolean) = {
    // NULL is only allowed in unique key, primary key does not allow NULL value
    val encodeResult = IndexKey.encodeIndexDataValues(
      row,
      index.getIndexColumns,
      handle,
      index.isUnique && !index.isPrimary,
      tiTableInfo)
    val keys = encodeResult.keys
    val indexKey =
      IndexKey.toIndexKey(WriteUtil.locatePhysicalTable(row, tiTableInfo), index.getId, keys: _*)
    (new SerializableKey(indexKey.getBytes), encodeResult.appendHandle)
  }

  private def generateRowKey(
      row: TiRow,
      handle: Handle,
      remove: Boolean): (SerializableKey, Array[Byte]) = {
    if (remove) {
      (buildRowKey(row, handle), new Array[Byte](0))
    } else {
      (
        new SerializableKey(
          RowKey.toRowKey(WriteUtil.locatePhysicalTable(row, tiTableInfo), handle).getBytes),
        encodeTiRow(row))
    }
  }

  private def generateRecordKV(
      rdd: ListBuffer[WrappedRow],
      remove: Boolean): ListBuffer[WrappedEncodedRow] = {
    rdd
      .map { row =>
        {
          val (encodedKey, encodedValue) = generateRowKey(row.row, row.handle, remove)
          WrappedEncodedRow(
            row.row,
            row.handle,
            encodedKey,
            encodedValue,
            isIndex = false,
            -1,
            remove)
        }
      }
  }
}
