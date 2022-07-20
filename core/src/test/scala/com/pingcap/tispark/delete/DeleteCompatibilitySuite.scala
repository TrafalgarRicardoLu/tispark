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

package com.pingcap.tispark.delete

import com.pingcap.tispark.datasource.BaseBatchWriteTest
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.Matchers.{
  an,
  be,
  contain,
  convertToAnyShouldWrapper,
  have,
  noException,
  not,
  the
}

/**
 *  This suite need to pass both tidb 4.x and 5.x
 *  5.x: test cluster index
 *  4.x: this suite can't test all cases because the config `alter-primary-key` can't be changed online.
 *
 */
class DeleteCompatibilitySuite extends BaseBatchWriteTest("test_delete_compatibility") {

  test("Delete cluster index table with int pk (pkIsHandle)") {
//    jdbcUpdate(
//      s"create table $dbtable(i int , s int, k int, PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")
//    spark.sql(s"insert into $dbtable values(0, 0,0),(1,1,1),(2,2,2),(1,2,2),(2,4,4),(10,5,5)")
    val table = "test_auth_basic"
    val database = "tispark_test_auth"
    val dbtable = f"$database.$table"
    jdbcUpdate(s"CREATE DATABASE IF NOT EXISTS `$database`")
    jdbcUpdate(s"create table IF NOT EXISTS $dbtable(i int unique key, s int unique key,k long)")
    spark.sql(s"insert into $dbtable values(1,2,1),(2,2,2)")
    spark.sql(s"select * from $dbtable").show()
    val schema =
      StructType(
        List(
          StructField("i", IntegerType, false),
          StructField("s", IntegerType),
          StructField("k", IntegerType)))
    val row1 = Row(1, 2, 3)
//    val row2 = Row(1, 1)
    val data: RDD[Row] = sc.makeRDD(List(row1))
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option("deduplicate", "false")
      .option("replace", "true")
      .mode("append")
      .save()
//    spark.sql(s"delete from $dbtable where i >= 2")

//    val expected = spark.sql(s"select count(*) from $dbtable where i < 2").head().get(0)
    val actual = spark.sql(s"select * from $dbtable").show()
    jdbcUpdate(s"drop table $dbtable")
//    assert(4 == actual)
  }

  test("Delete cluster index table with varchar pk (commonIsHandle)") {
    jdbcUpdate(
      s"create table $dbtable(i varchar(64), s int,PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")
    jdbcUpdate(s"insert into $dbtable values('0',0),('1',1),('2',2),('3',3)")

    spark.sql(s"delete from $dbtable where i >= '2'")

    val expected = spark.sql(s"select count(*) from $dbtable where i < '2'").head().get(0)
    val actual = spark.sql(s"select count(*) from $dbtable").head().get(0)
    assert(expected == actual)
  }

  test("Delete non-cluster index table with int pk") {
    jdbcUpdate(
      s"create table $dbtable(i int, s int,PRIMARY KEY (i)/*T![clustered_index] NONCLUSTERED */,unique key (s))")
    jdbcUpdate(s"insert into $dbtable values(0,0),(1,1),(2,2),(3,3)")

    spark.sql(s"delete from $dbtable where i >= 2")

    val expected = spark.sql(s"select count(*) from $dbtable where i < 2").head().get(0)
    val actual = spark.sql(s"select count(*) from $dbtable").head().get(0)
    assert(expected == actual)
  }

  test("Delete non-cluster index table with varchar pk") {
    jdbcUpdate(
      s"create table $dbtable(i varchar(64), s int,PRIMARY KEY (i)/*T![clustered_index] NONCLUSTERED */)")
    jdbcUpdate(s"insert into $dbtable values('0',0),('1',1),('2',2),('3',3)")

    spark.sql(s"delete from $dbtable where i >= '2'")

    val expected = spark.sql(s"select count(*) from $dbtable where i < '2'").head().get(0)
    val actual = spark.sql(s"select count(*) from $dbtable").head().get(0)
    assert(expected == actual)
  }

  test("Delete table without pk") {
    jdbcUpdate(s"create table $dbtable(i int, s int ,unique key (s))")
    jdbcUpdate(s"insert into $dbtable values(0,0),(1,1),(2,2),(3,3)")

    spark.sql(s"delete from $dbtable where i >= 2")

    val expected = spark.sql(s"select count(*) from $dbtable where i < 2").head().get(0)
    val actual = spark.sql(s"select count(*) from $dbtable").head().get(0)
    assert(expected == actual)
  }

  test("Select after delete should not contains _tidb_rowid if it does not before") {
    jdbcUpdate(
      s"create table $dbtable(i varchar(64), s int,PRIMARY KEY (i)/*T![clustered_index] NONCLUSTERED */,unique key (s))")
    jdbcUpdate(s"insert into $dbtable values('0',0),('1',1),('2',2),('3',3)")

    val df1 = spark.sql(s"select * from $dbtable")
    spark.sql(s"delete from $dbtable where i >= '2'")
    val df2 = spark.sql(s"select * from $dbtable")

    assert(
      df1.schema.fieldNames
        .contains("_tidb_rowid") || (!df2.schema.fieldNames.contains("_tidb_rowid")))
  }
}
