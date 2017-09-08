/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  * <p>
  * http://www.apache.org/licenses/LICENSE-2.0
  * <p>
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package com.aliyun.odps.spark.examples

import com.aliyun.odps.data.Record
import com.aliyun.odps.{PartitionSpec, TableSchema}
import org.apache.spark.odps.OdpsOps
import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
  * 1. build aliyun-cupid-sdk
  * 2. properly set spark.defaults.conf
  * 3. bin/spark-submit --master yarn-cluster --class com.aliyun.odps.spark.examples.OdpsTableReadWrite \
  * /path/to/aliyun-cupid-sdk/examples/spark-examples/target/spark-examples_2.11-1.0.0-SNAPSHOT-shaded.jar
  */
object OdpsTableReadWrite {
  /**
    * This Demo represent the ODPS Table Read/Write Usage in following scenarios
    * This Demo will fail if you don't have such Table inside such Project
    *   1. read from normal table via rdd api
    *   2. read from single partition column table via rdd api
    *   3. read from multi partition column table via rdd api
    *   4. read with multi partitionSpec definition via rdd api
    *   5. save rdd into normal table
    *   6. save rdd into partition table with single partition spec
    *   7. dynamic save rdd into partition table with multiple partition spec
    *
    */
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("OdpsTableReadWrite")
      .getOrCreate()

    val sc = spark.sparkContext
    val projectName = sc.getConf.get("odps.project.name")

    try {
      val odpsOps = new OdpsOps(sc)

      /**
        * read from normal table via rdd api
        * desc cupid_wordcount;
        * +------------------------------------------------------------------------------------+
        * | Field           | Type       | Label | Comment                                     |
        * +------------------------------------------------------------------------------------+
        * | id              | string     |       |                                             |
        * | value           | string     |       |                                             |
        * +------------------------------------------------------------------------------------+
        */
      val rdd_0 = odpsOps.readTable(
        projectName,
        "cupid_wordcount",
        (r: Record, schema: TableSchema) => (r.getString(0), r.getString(1))
      )

      /**
        * read from single partition column table via rdd api
        * desc dftest_single_parted;
        * +------------------------------------------------------------------------------------+
        * | Field           | Type       | Label | Comment                                     |
        * +------------------------------------------------------------------------------------+
        * | id              | string     |       |                                             |
        * | value           | string     |       |                                             |
        * +------------------------------------------------------------------------------------+
        * | Partition Columns:                                                                 |
        * +------------------------------------------------------------------------------------+
        * | pt              | string     |                                                     |
        * +------------------------------------------------------------------------------------+
        */
      val rdd_1 = odpsOps.readTable(
        projectName,
        "dftest_single_parted",
        Array("pt=20160101"),
        (r: Record, schema: TableSchema) => (r.getString(0), r.getString(1), r.getString("pt"))
      )

      /**
        * read from multi partition column table via rdd api
        * desc dftest_parted;
        * +------------------------------------------------------------------------------------+
        * | Field           | Type       | Label | Comment                                     |
        * +------------------------------------------------------------------------------------+
        * | id              | string     |       |                                             |
        * | value           | string     |       |                                             |
        * +------------------------------------------------------------------------------------+
        * | Partition Columns:                                                                 |
        * +------------------------------------------------------------------------------------+
        * | pt              | string     |                                                     |
        * | hour            | string     |                                                     |
        * +------------------------------------------------------------------------------------+
        */
      val rdd_2 = odpsOps.readTable(
        projectName,
        "dftest_parted",
        Array("pt=20160101,hour=12"),
        (r: Record, schema: TableSchema) => (r.getString(0), r.getString(1), r.getString("pt"), r.getString(3))
      )

      /**
        * read with multi partitionSpec definition via rdd api
        * desc cupid_partition_table1;
        * +------------------------------------------------------------------------------------+
        * | Field           | Type       | Label | Comment                                     |
        * +------------------------------------------------------------------------------------+
        * | id              | string     |       |                                             |
        * | value           | string     |       |                                             |
        * +------------------------------------------------------------------------------------+
        * | Partition Columns:                                                                 |
        * +------------------------------------------------------------------------------------+
        * | pt1             | string     |                                                     |
        * | pt2             | string     |                                                     |
        * +------------------------------------------------------------------------------------+
        */
      val rdd_3 = odpsOps.readTable(
        projectName,
        "cupid_partition_table1",
        Array("pt1=part1,pt2=part1", "pt1=part1,pt2=part2", "pt1=part2,pt2=part3"),
        (r: Record, schema: TableSchema) => (r.getString(0), r.getString(1), r.getString("pt1"), r.getString("pt2"))
      )

      /**
        * save rdd into normal table
        * desc cupid_wordcount_empty;
        * +------------------------------------------------------------------------------------+
        * | Field           | Type       | Label | Comment                                     |
        * +------------------------------------------------------------------------------------+
        * | id              | string     |       |                                             |
        * | value           | string     |       |                                             |
        * +------------------------------------------------------------------------------------+
        */
      val transfer_0 = (v: Tuple2[String, String], record: Record, schema: TableSchema) => {
        record.set("id", v._1)
        record.set(1, v._2)
      }
      odpsOps.saveToTable(projectName, "cupid_wordcount_empty", rdd_0, transfer_0, true)

      /**
        * save rdd into partition table with single partition spec
        * desc cupid_partition_table1;
        * +------------------------------------------------------------------------------------+
        * | Field           | Type       | Label | Comment                                     |
        * +------------------------------------------------------------------------------------+
        * | id              | string     |       |                                             |
        * | value           | string     |       |                                             |
        * +------------------------------------------------------------------------------------+
        * | Partition Columns:                                                                 |
        * +------------------------------------------------------------------------------------+
        * | pt1             | string     |                                                     |
        * | pt2             | string     |                                                     |
        * +------------------------------------------------------------------------------------+
        */
      val transfer_1 = (v: Tuple2[String, String], record: Record, schema: TableSchema) => {
        record.set("id", v._1)
        record.set("value", v._2)
      }
      odpsOps.saveToTable(projectName, "cupid_partition_table1", "pt1=test,pt2=dev", rdd_0, transfer_1, true)

      /**
        * dynamic save rdd into partition table with multiple partition spec
        * desc cupid_partition_table1;
        * +------------------------------------------------------------------------------------+
        * | Field           | Type       | Label | Comment                                     |
        * +------------------------------------------------------------------------------------+
        * | id              | string     |       |                                             |
        * | value           | string     |       |                                             |
        * +------------------------------------------------------------------------------------+
        * | Partition Columns:                                                                 |
        * +------------------------------------------------------------------------------------+
        * | pt1             | string     |                                                     |
        * | pt2             | string     |                                                     |
        * +------------------------------------------------------------------------------------+
        */
      val transfer_2 = (v: Tuple2[String, String], record: Record, part: PartitionSpec, schema: TableSchema) => {
        record.set("id", v._1)
        record.set("value", v._2)

        val pt1_value = if (new Random().nextInt(10) % 2 == 0) "even" else "odd"
        val pt2_value = if (new Random().nextInt(10) % 2 == 0) "even" else "odd"
        part.set("pt1", pt1_value)
        part.set("pt2", pt2_value)
      }
      odpsOps.saveToTableForMultiPartition(projectName, "cupid_partition_table1", rdd_0, transfer_2, true)
    } catch {
      case ex: Exception => {
        throw ex
      }
    } finally {
      sc.stop
    }
  }
}
