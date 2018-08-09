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

package org.apache.spark.odps

import java.lang
import java.util.Date

import com.aliyun.odps.account.AliyunAccount
import com.aliyun.odps.cupid.requestcupid.DDLTaskUtil
import com.aliyun.odps.cupid.runtime.{InputOutputInfosManager, RuntimeContext}
import com.aliyun.odps.cupid.tools.Logging
import com.aliyun.odps.data.Record
import com.aliyun.odps.tunnel.TableTunnel
import com.aliyun.odps.{Odps, PartitionSpec, TableSchema}
import org.apache.spark.rdd.{OdpsRDD, OdpsRDDActions, RDD}
import org.apache.spark.{SparkContext, SparkException, TaskContext}

import scala.collection.mutable.{ArrayBuffer, Set}
import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.reflect.{ClassTag, classTag}
import java.util.UUID

import com.aliyun.odps.cupid.CupidSession


/**
  * @author liwei.li
  */
class OdpsOps(@transient sc: SparkContext)
  extends Logging with Serializable {
  val accessId: String = sc.getConf.get("odps.access.id", null)
  val accessKey: String = sc.getConf.get("odps.access.key", null)
  val odpsUrl: String = if (sc.isLocal) sc.getConf.get("odps.end.point") else
    sc.getConf.get("odps.runtime.end.point", sc.getConf.get("odps.end.point"))

  private def ParsePartSpec(partSpecIn: String): String = {
    val parsePartSpec = new PartitionSpec(partSpecIn)
    val parsePartOut = parsePartSpec.toString().trim().replaceAll("'", "")
    parsePartOut
  }

  private def outPutPartitionInfo(partition: Array[String]) {
    val outPartition = partition.take(3)
    var outMesg = "read partition num = " + partition.length + ",the first 3 partition is\n"
    outPartition.foreach(f => outMesg = outMesg + f + "\n")
    logInfo(outMesg)
  }

  private def isTableStaEnable(): Boolean = {
    return sc.getConf.getBoolean("odps.cupid.table.statistic.enable", true)
  }

  def readTable[T: ClassTag](project: String, table: String, partition: Array[String],
                             transfer: (Record, TableSchema) => T,
                             numPartition: Int): RDD[T] = {
    readTable(project, table, partition, Array[String](), transfer, numPartition)
  }

  def readTable[T: ClassTag](project: String, table: String, partition: Array[String], columns: Array[String],
                             transfer: (Record, TableSchema) => T,
                             numPartition: Int): RDD[T] = {
    outPutPartitionInfo(partition)

    val func = sc.clean(transfer)
    var partSpecOut = ArrayBuffer[String]()
    for (partitiontmp <- partition) {
      partSpecOut += ParsePartSpec(partitiontmp)
    }
    val rdd = if (numPartition > 0) {
      new OdpsRDD[T](sc, accessId, accessKey, odpsUrl,
        project, table, partSpecOut.toArray, numPartition, func, sc.isLocal, columns)
    } else {
      new OdpsRDD[T](sc, accessId, accessKey, odpsUrl,
        project, table, partSpecOut.toArray, 0, func, sc.isLocal, columns, sc.conf.getInt("odps.input.split.size", 256))
    }
    if(!sc.isLocal && isTableStaEnable()) {
      OdpsOps.addInputItem(rdd.id.toString, project, table, partition)
    }

    rdd
  }

  def readTable[T: ClassTag](project: String, table: String, partition: Array[String],
                             transfer: (Record, TableSchema) => T): RDD[T] = {
    readTable(project, table, partition, Array[String](), transfer, 0)
  }

  def readTable[T: ClassTag](project: String, table: String, partition: String,
                             transfer: (Record, TableSchema) => T,
                             numPartition: Int): RDD[T] = {
    if (partition != "") {
      readTable(project, table, Array(partition), Array[String](), transfer, numPartition)
    } else {
      readTable(project, table, Array[String](), Array[String](), transfer, numPartition)
    }
  }

  def readTable[T: ClassTag](project: String, table: String, partition: String,
                             transfer: (Record, TableSchema) => T): RDD[T] = {
    readTable(project, table, partition, transfer, 0)
  }

  def readTable[T: ClassTag](project: String, table: String,
                             transfer: (Record, TableSchema) => T,
                             numPartition: Int = 0): RDD[T] = {
    readTable(project, table, Array[String](), Array[String](), transfer, numPartition)
  }

  /*
   * for ft some cases we preserve these two methods
   */
  @deprecated
  def readTable[T: ClassTag](path: String, columns: Array[String], partSpec: Array[String], transfer: (Record, TableSchema) => T): RDD[T] = {
    logWarning("ATTENTION: columns is not supported so far!!")
    val odpsProtocol = "odps://"
    if (!path.startsWith(odpsProtocol)) throw new SparkException("Odps table schema error: " + path)
    val tblPath = path.drop(odpsProtocol.length).split("/")
    val projectStr = tblPath(0)
    val tableStr = tblPath(1)
    readTable(projectStr, tableStr, partSpec, transfer)
  }

  /*
   * for ft some cases we preserve these two methods
   */
  @deprecated
  def readTable[T: ClassTag](path: String, columns: Array[String], partSpec: String, transfer: (Record, TableSchema) => T): RDD[T] = {
    if (partSpec != "") {
      readTable(path, columns, Array(partSpec), transfer)
    } else {
      readTable(path, columns, Array[String](), transfer)
    }
  }

  def saveToTable[T: ClassTag](project: String, table: String, partition: String,
                               rdd: RDD[T],
                               transfer: (T, Record, TableSchema) => Unit, isOverWrite: Boolean = false) {
    if (!sc.isLocal) {
      if (isTableStaEnable()) {
        OdpsOps.addOutputItem(rdd.id.toString + "_" + UUID.randomUUID().toString(), project, table, ArrayBuffer(partition).toArray)
      }
      var partSpecOut = partition
      if (partition != "") {
        partSpecOut = ParsePartSpec(partition)
      }

      val odpsRddActions: OdpsRDDActions[T] = OdpsRDDActions.fromRDD(rdd)
      odpsRddActions.save(table, project, transfer, if (isOverWrite) java.lang.Boolean.TRUE else java.lang.Boolean.FALSE, partSpecOut)
    } else {
      if (isOverWrite == true)
        logError("overwrite mode is not supported by local mode")
      if (partition == "")
        tunnelSaveTable(project, table, rdd, transfer)
      else
        tunnelSaveTable(project, table, partition, rdd, transfer)
    }
  }

  private def getSaveMultiPartitionPartSpec( ddlTaskPaths: Set[String]):Array[String] = {
    val ddlTaskPathsArray = ddlTaskPaths.toArray
    var partSpecs: ArrayBuffer[String] = ArrayBuffer[String]()
    //ddlTaskPath is save temp dir,eg:pangu://../.FuxiJobTempRoot/../saveId/../partSpecStr
    for (ddlTaskPath <- ddlTaskPathsArray) {
      val pathParams = ddlTaskPath.split("/")
      val pathPectOut = ParsePartSpec(pathParams(pathParams.length - 1))
      partSpecs += pathPectOut
    }
    partSpecs.toArray
  }

  def saveToTableForMultiPartition[T: ClassTag](project: String, table: String, rdd: RDD[T],
                                                transfer: (T, Record, PartitionSpec, TableSchema) => Unit, isOverWrite: Boolean = false) {
    if (!sc.isLocal) {
      val odpsRddActions: OdpsRDDActions[T] = OdpsRDDActions.fromRDD(rdd)
      odpsRddActions.save(table, project, transfer, if (isOverWrite) java.lang.Boolean.TRUE else java.lang.Boolean.FALSE, "")
    } else {
      logError("saveToTableForMultiPartition mode is not supported by local mode")
    }

  }

  def saveToTable[T: ClassTag](project: String, table: String,
                               rdd: RDD[T],
                               transfer: (T, Record, TableSchema) => Unit) {
    saveToTable(project, table, "", rdd, transfer, false)
  }

  def saveToTable[T: ClassTag](project: String, table: String,
                               rdd: RDD[T],
                               transfer: (T, Record, TableSchema) => Unit,
                               isOverWrite: Boolean) {
    saveToTable(project, table, "", rdd, transfer, isOverWrite)
  }

  @unchecked
  private def tunnelSaveTable[T: ClassTag](project: String, table: String, partition: String,
                                           rdd: RDD[T],
                                           transfer: (T, Record, TableSchema) => Unit) {
    def transfer0(t: T, record: Record, scheme: TableSchema): Record = {
      transfer(t, record, scheme)
      record
    }
    val func = sc.clean(transfer0 _)
    val odps = CupidSession.get().odps()
    odps.setDefaultProject(project)
    odps.setEndpoint(odpsUrl)
    val tunnel = new TableTunnel(odps)
    val partitionSpec = new PartitionSpec(partition)
    val uploadSession = tunnel.createUploadSession(project, table, partitionSpec)
    logInfo("Odps upload session status is: " + uploadSession.getStatus.toString)
    val uploadId = uploadSession.getId

    def writeToFile(context: TaskContext, iter: Iterator[T]) {
      val odps_ = CupidSession.get().odps()
      odps_.setDefaultProject(project)
      odps_.setEndpoint(odpsUrl)
      val tunnel_ = new TableTunnel(odps_)
      val partitionSpec_ = new PartitionSpec(partition)
      val uploadSession_ = tunnel_.getUploadSession(project, table, partitionSpec_, uploadId)
      val writer = uploadSession_.openRecordWriter(context.partitionId)
      while (iter.hasNext) {
        val value = iter.next()
        logDebug("context id: " + context.partitionId + " write: " + value)
        writer.write(func(value, uploadSession_.newRecord(), uploadSession_.getSchema))
      }
      logDebug("ready context id: " + context.partitionId)
      writer.close()
      logDebug("finish context id: " + context.partitionId)
    }

    sc.runJob(rdd, writeToFile _)
    val arr = Array.tabulate(rdd.partitions.length)(l => Long.box(l))
    uploadSession.commit(arr)
  }

  @unchecked
  private def tunnelSaveTable[T: ClassTag](project: String, table: String,
                                           rdd: RDD[T],
                                           transfer: (T, Record, TableSchema) => Unit) {
    def transfer0(t: T, record: Record, scheme: TableSchema): Record = {
      transfer(t, record, scheme)
      record
    }
    val func = sc.clean(transfer0 _)
    val odps = CupidSession.get().odps()
    odps.setDefaultProject(project)
    odps.setEndpoint(odpsUrl)
    val tunnel = new TableTunnel(odps)
    val uploadSession = tunnel.createUploadSession(project, table)
    logInfo("Odps upload session status is: " + uploadSession.getStatus.toString)
    val uploadId = uploadSession.getId

    def writeToFile(context: TaskContext, iter: Iterator[T]) {
      val odps_ = CupidSession.get().odps()
      odps_.setDefaultProject(project)
      odps_.setEndpoint(odpsUrl)
      val tunnel_ = new TableTunnel(odps_)
      val uploadSession_ = tunnel_.getUploadSession(project, table, uploadId)
      val writer = uploadSession_.openRecordWriter(context.partitionId)
      while (iter.hasNext) {
        val value = iter.next()
        logDebug("context id: " + context.partitionId + " write: " + value)
        writer.write(func(value, uploadSession_.newRecord(), uploadSession_.getSchema))
      }
      logDebug("ready context id: " + context.partitionId)
      writer.close()
      logDebug("finish context id: " + context.partitionId)
    }

    sc.runJob(rdd, writeToFile _)
    val arr = Array.tabulate(rdd.partitions.length)(l => Long.box(l))
    uploadSession.commit(arr)
  }

}

/**
  * usage: import org.apache.spark.odps.OdpsOps._
  * more safe and more convinient boxed odps Record
  */
object OdpsOps extends Logging {
  implicit def apply(sc: SparkContext) = {
    new OdpsOps(sc)
  }

  val infosManager = InputOutputInfosManager.getInstance()

  def addInputItem(stageId: String,
                   project: String,
                   table: String,
                   partition: Array[String]): Unit = {

    infosManager.addInputItem(stageId, getTableInfo(project, table, partition))
    updateInputOutputInfosToMaster()
  }

  def addOutputItem(stageId: String,
                    project: String,
                    table: String,
                    partition: Array[String]): Unit = {
    infosManager.addOutputItem(stageId, getTableInfo(project, table, partition))
    updateInputOutputInfosToMaster()
  }

  /**
    * @param project
    * @param table
    * @param partition Array("pt1=p1,pt2=p1","pt1=p2,pt2=p2")
    * @return project.table/pt1=p1/pt2=p1,project.table/pt1=p2/pt2=p2
    */
  private def getTableInfo(project: String, table: String, partition: Array[String]): String = {
    val tableInfo = StringBuilder.newBuilder
    if (partition == null || partition.length == 0) {
      tableInfo.append(project)
        .append(".")
        .append(table)
      tableInfo.toString()
    } else {
      for (p <- partition) {
        tableInfo.append(project)
          .append(".")
          .append(table)
          .append("/")
          .append(p.replace(",", "/"))
          .append(",")
      }
      tableInfo.substring(0, tableInfo.length - 1).toString
    }
  }

  private def updateInputOutputInfosToMaster(): Unit = {
    try {
      RuntimeContext.get().updateInputOutputInfos(infosManager)
    } catch {
      case e: Exception =>
        logWarning("Failed to update input/output info to master: " + infosManager.getSummary, e)
    }
  }

  implicit class RichRecord(record: Record) {
    def getBigintOptional(columnName: String): Option[Long] = {
      val columnValue: lang.Long = record.getBigint(columnName)
      if (columnValue == null) {
        None
      } else {
        Some(columnValue)
      }
    }

    def getBigintOrElse(columnName: String, defaultValue: Long): Long = {
      val columnValue: lang.Long = record.getBigint(columnName)
      if (columnValue == null) defaultValue else columnValue
    }

    def getBigintOptional(columnIndex: Int): Option[Long] = {
      val columnValue: lang.Long = record.getBigint(columnIndex)
      if (columnValue == null) {
        None
      } else {
        Some(columnValue)
      }
    }

    def getBigintOrElse(columnIndex: Int, defaultValue: Long): Long = {
      val columnValue: lang.Long = record.getBigint(columnIndex)
      if (columnValue == null) defaultValue else columnValue
    }

    def getStringOptional(columnName: String): Option[String] = {
      val columnValue: String = record.getString(columnName)
      if (columnValue == null) None else Some(columnValue)
    }

    def getStringOrElse(columnName: String, defaultValue: String): String = {
      val columnValue: String = record.getString(columnName)
      if (columnValue == null) defaultValue else columnValue
    }

    def getStringOptional(columnIndex: Int): Option[String] = {
      val columnValue: String = record.getString(columnIndex)
      if (columnValue == null) None else Some(columnValue)
    }

    def getStringOrElse(columnIndex: Int, defaultValue: String): String = {
      val columnValue: String = record.getString(columnIndex)
      if (columnValue == null) defaultValue else columnValue
    }

    def getDoubleOptional(columnName: String): Option[Double] = {
      val columnValue: lang.Double = record.getDouble(columnName)
      if (columnValue == null) {
        None
      } else {
        Some(columnValue)
      }
    }

    def getDoubleOrElse(columnName: String, defaultValue: Double): Double = {
      val columnValue: lang.Double = record.getDouble(columnName)
      if (columnValue == null) defaultValue else columnValue
    }

    def getDoubleOptional(columnIndex: Int): Option[Double] = {
      val columnValue: lang.Double = record.getDouble(columnIndex)
      if (columnValue == null) {
        None
      } else {
        Some(columnValue)
      }
    }

    def getDoubleOrElse(columnIndex: Int, defaultValue: Double): Double = {
      val columnValue: lang.Double = record.getDouble(columnIndex)
      if (columnValue == null) defaultValue else columnValue
    }

    def getBooleanOptional(columnName: String): Option[Boolean] = {
      val columnValue: lang.Boolean = record.getBoolean(columnName)
      if (columnValue == null) {
        None
      } else {
        Some(columnValue)
      }
    }

    def getBooleanOrElse(columnName: String, defaultValue: Boolean): Boolean = {
      val columnValue: lang.Boolean = record.getBoolean(columnName)
      if (columnValue == null) defaultValue else columnValue
    }

    def getBooleanOptional(columnIndex: Int): Option[Boolean] = {
      val columnValue: lang.Boolean = record.getBoolean(columnIndex)
      if (columnValue == null) {
        None
      } else {
        Some(columnValue)
      }
    }

    def getBooleanOrElse(columnIndex: Int, defaultValue: Boolean): Boolean = {
      val columnValue: lang.Boolean = record.getBoolean(columnIndex)
      if (columnValue == null) defaultValue else columnValue
    }

    def getDatetimeOptional(columnName: String): Option[Date] = {
      val columnValue: Date = record.getDatetime(columnName)
      if (columnValue == null) None else Some(columnValue)
    }

    def getDatetimeOrElse(columnName: String, defaultValue: Date): Date = {
      val columnValue: Date = record.getDatetime(columnName)
      if (columnValue == null) defaultValue else columnValue
    }

    def getDatetimeOptional(columnIndex: Int): Option[Date] = {
      val columnValue: Date = record.getDatetime(columnIndex)
      if (columnValue == null) None else Some(columnValue)
    }

    def getDatetimeOrElse(columnIndex: Int, defaultValue: Date): Date = {
      val columnValue: Date = record.getDatetime(columnIndex)
      if (columnValue == null) defaultValue else columnValue
    }

    def getDecimalOptional(columnName: String): Option[BigDecimal] = {
      val columnValue: BigDecimal = record.getDecimal(columnName)
      if (columnValue == null) None else Some(columnValue)
    }

    def getDecimalOrElse(columnName: String, defaultValue: BigDecimal): BigDecimal = {
      val columnValue: BigDecimal = record.getDecimal(columnName)
      if (columnValue == null) defaultValue else columnValue
    }

    def getDecimalOptional(columnIndex: Int): Option[BigDecimal] = {
      val columnValue: BigDecimal = record.getDecimal(columnIndex)
      if (columnValue == null) None else Some(columnValue)
    }

    def getDecimalOrElse(columnIndex: Int, defaultValue: BigDecimal): BigDecimal = {
      val columnValue: BigDecimal = record.getDecimal(columnIndex)
      if (columnValue == null) defaultValue else columnValue
    }
  }

}
