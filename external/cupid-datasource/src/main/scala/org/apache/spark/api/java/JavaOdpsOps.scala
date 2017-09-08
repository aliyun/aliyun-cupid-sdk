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

package org.apache.spark.api.java

import com.aliyun.odps.data.Record
import com.aliyun.odps.{PartitionSpec, TableSchema}
import org.apache.spark.api.java.JavaPairRDD._
import org.apache.spark.api.java.function.{Function2 => JFunction2, Function3 => JFunction3, VoidFunction3 => JVoidFunction3, VoidFunction4 => JVoidFunction4}
import org.apache.spark.odps.OdpsOps
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class JavaOdpsOps(sc: JavaSparkContext) {

  private[spark] implicit def toScalaVoidFunction3[T1, T2, T3](fun: JVoidFunction3[T1, T2, T3]): (T1, T2, T3) => Unit = {
    (x: T1, x1: T2, x2: T3) => fun.call(x, x1, x2)
  }

  private[spark] implicit def toScalaVoidFunction4[T1, T2, T3, T4](fun: JVoidFunction4[T1, T2, T3, T4]): (T1, T2, T3, T4) => Unit = {
    (x: T1, x1: T2, x2: T3, x3: T4) => fun.call(x, x1, x2, x3)
  }

  val odpsOps = new OdpsOps(sc.sc)

  def readTable[T](project: String, table: String, partition: Array[String],
                   transfer: JFunction2[Record, TableSchema, T],
                   numPartition: Int): JavaRDD[T] = {
    readTable(project, table, partition, Array[String](), transfer, numPartition)
  }

  def readTable[T](project: String, table: String, partition: Array[String],
                   transfer: JFunction2[Record, TableSchema, T]): JavaRDD[T] = {
    readTable(project, table, partition, Array[String](), transfer, 0)
  }

  def readTable[T](project: String, table: String, partition: String,
                   transfer: JFunction2[Record, TableSchema, T]): JavaRDD[T] = {
    readTable(project, table, partition, transfer, 0)
  }

  def readTable[T](project: String, table: String, partition: String,
                   transfer: JFunction2[Record, TableSchema, T],
                   numPartition: Int): JavaRDD[T] = {
    if (partition != "") {
      readTable(project, table, Array(partition), Array[String](), transfer, numPartition)
    } else {
      readTable(project, table, Array[String](), Array[String](), transfer, numPartition)
    }
  }

  def readTable[T](project: String, table: String, partition: Array[String], columns: Array[String],
                   transfer: JFunction2[Record, TableSchema, T],
                   numPartition: Int): JavaRDD[T] = {
    odpsOps.readTable(project, table, partition, columns, transfer, numPartition)(fakeClassTag).toJavaRDD()
  }

  def fakeClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]

  def readTable[T](project: String, table: String, transfer: JFunction2[Record, TableSchema, T],
                   numPartition: Int = 0): JavaRDD[T] = {
    readTable(project, table, Array[String](), Array[String](), transfer, numPartition)
  }

  def saveToTable[T](project: String, table: String, partition: String,
                     rdd: RDD[T],
                     transfer: JVoidFunction3[T, Record, TableSchema]): Unit = {
    saveToTable(project, table, partition, rdd, transfer, false)
  }

  def saveToTable[T](project: String, table: String,
                     rdd: RDD[T],
                     transfer: JVoidFunction3[T, Record, TableSchema], isOverWrite: Boolean = false): Unit = {
    saveToTable(project, table, "", rdd, transfer, isOverWrite)
  }

  def saveToTable[T](project: String, table: String, partition: String,
                     rdd: RDD[T],
                     transfer: JVoidFunction3[T, Record, TableSchema], isOverWrite: Boolean) {
    odpsOps.saveToTable(project, table, partition, rdd, transfer, isOverWrite)(fakeClassTag)
  }

  def saveToTableForMultiPartition[T](project: String, table: String, rdd: RDD[T],
                                      transfer: JVoidFunction4[T, Record, PartitionSpec, TableSchema]) {
    saveToTableForMultiPartition(project, table, rdd, transfer, false)

  }

  def saveToTableForMultiPartition[T](project: String, table: String, rdd: RDD[T],
                                      transfer: JVoidFunction4[T, Record, PartitionSpec, TableSchema], isOverWrite: Boolean) {
    odpsOps.saveToTableForMultiPartition(project, table, rdd, transfer, isOverWrite)(fakeClassTag)
  }

}
