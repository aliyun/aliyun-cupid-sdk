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

import java.io.IOException
import java.util.{Date, UUID}
import java.{lang, util}

import apsara.odps.cupid.protocol.SavePathsProtos.{SavePathIterm, SavePaths}
import com.aliyun.odps.`type`.TypeInfoParser
import com.aliyun.odps.account.AliyunAccount
import com.aliyun.odps.cupid.requestcupid.DDLTaskUtil
import com.aliyun.odps.cupid.runtime.{InputOutputInfosManager, NextIterator, RuntimeContext, TableWriterAbstract}
import com.aliyun.odps.cupid.tools.Logging
import com.aliyun.odps.cupid.{CupidSession, CupidTaskInterativeUtil, CupidUtil}
import com.aliyun.odps.data.{ArrayRecord, Record}
import com.aliyun.odps.tunnel.TableTunnel
import com.aliyun.odps.{Column, Odps, PartitionSpec, TableSchema}
import org.apache.spark.executor.CommitDeniedException
import org.apache.spark.rdd.{OdpsRDD, RDD}
import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.{SparkContext, SparkEnv, SparkException, TaskContext}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, Set}
import scala.language.implicitConversions
import scala.reflect.ClassTag

class OdpsOps(@transient sc: SparkContext)
  extends Logging with Serializable {
  val accessId: String = sc.getConf.get("odps.access.id")
  val accessKey: String = sc.getConf.get("odps.access.key")
  val odpsUrl: String = sc.getConf.get("odps.end.point")

  def readTable[T: ClassTag](project: String, table: String, partition: Array[String],
                             transfer: (Record, TableSchema) => T,
                             numPartition: Int): RDD[T] = {
    readTable(project, table, partition, Array[String](), transfer, numPartition)
  }

  def readTable[T: ClassTag](project: String, table: String, partition: String,
                             transfer: (Record, TableSchema) => T): RDD[T] = {
    readTable(project, table, partition, transfer, 0)
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

  def readTable[T: ClassTag](project: String, table: String,
                             transfer: (Record, TableSchema) => T,
                             numPartition: Int = 0): RDD[T] = {
    readTable(project, table, Array[String](), Array[String](), transfer, numPartition)
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

  def readTable[T: ClassTag](project: String, table: String, partition: Array[String],
                             transfer: (Record, TableSchema) => T): RDD[T] = {
    readTable(project, table, partition, Array[String](), transfer, 0)
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
    if (!sc.isLocal)
      OdpsOps.addInputItem(rdd.id.toString, project, table, partition)

    rdd
  }

  private def outPutPartitionInfo(partition: Array[String]) {
    val outPartition = partition.take(3)
    var outMesg = "read partition num = " + partition.length + ",the first 3 partition is\n"
    outPartition.foreach(f => outMesg = outMesg + f + "\n")
    logInfo(outMesg)
  }

  def saveToTableForMultiPartition[T: ClassTag](project: String, table: String, rdd: RDD[T],
                                                transfer: (T, Record, PartitionSpec, TableSchema) => Unit, isOverWrite: Boolean = false) {
    if (!sc.isLocal) {
      val ddlTaskPaths = clusterSaveTable(table, project, rdd, transfer, isOverWrite)
      logInfo("use save multipartiton,partition num = " + ddlTaskPaths.size)
      if (ddlTaskPaths.size != 0) {
        val tablePartPecs = getSaveMultiPartitionPartSpec(ddlTaskPaths)
        OdpsOps.addOutputItem(rdd.id.toString + "_" + UUID.randomUUID().toString(), project, table, tablePartPecs)
        DDLTaskUtil.doDDLTaskForSaveMultiParittion(project, table, isOverWrite, ddlTaskPaths)
      } else
        logWarning("the save multipartition do not have data")

    } else {
      logError("saveToTableForMultiPartition mode is not supported by local mode")
    }

  }

  private def getSaveMultiPartitionPartSpec(ddlTaskPaths: Set[String]): Array[String] = {
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

  def saveToTable[T: ClassTag](project: String, table: String, partition: String,
                               rdd: RDD[T],
                               transfer: (T, Record, TableSchema) => Unit, isOverWrite: Boolean = false) {
    if (!sc.isLocal) {
      OdpsOps.addOutputItem(rdd.id.toString + "_" + UUID.randomUUID().toString(), project, table, ArrayBuffer(partition).toArray)
      var partSpecOut = partition
      if (partition != "") {
        partSpecOut = ParsePartSpec(partition)
      }
      val ddlTaskPaths = clusterSaveTable(table, project, rdd, transfer, isOverWrite).toArray
      logInfo("saveToTable ddlTaskPaths.size = " + ddlTaskPaths.size)
      if (!ddlTaskPaths.isEmpty) {
        DDLTaskUtil.doDDLTaskForNoarmalSave(project, table, partSpecOut, isOverWrite, ddlTaskPaths(0))
      }

    } else {
      if (isOverWrite == true)
        logError("overwrite mode is not supported by local mode")
      if (partition == "")
        tunnelSaveTable(project, table, rdd, transfer)
      else
        tunnelSaveTable(project, table, partition, rdd, transfer)
    }
  }

  private def ParsePartSpec(partSpecIn: String): String = {
    val parsePartSpec = new PartitionSpec(partSpecIn)
    val parsePartOut = parsePartSpec.toString().trim().replaceAll("'", "")
    parsePartOut
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
    val account = new AliyunAccount(accessId, accessKey)
    val odps = new Odps(account)
    odps.setDefaultProject(project)
    odps.setEndpoint(odpsUrl)
    val tunnel = new TableTunnel(odps)
    val partitionSpec = new PartitionSpec(partition)
    val uploadSession = tunnel.createUploadSession(project, table, partitionSpec)
    logInfo("Odps upload session status is: " + uploadSession.getStatus.toString)
    val uploadId = uploadSession.getId

    def writeToFile(context: TaskContext, iter: Iterator[T]) {
      val account_ = new AliyunAccount(accessId, accessKey)
      val odps_ = new Odps(account_)
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
    val account = new AliyunAccount(accessId, accessKey)
    val odps = new Odps(account)
    odps.setDefaultProject(project)
    odps.setEndpoint(odpsUrl)
    val tunnel = new TableTunnel(odps)
    val uploadSession = tunnel.createUploadSession(project, table)
    logInfo("Odps upload session status is: " + uploadSession.getStatus.toString)
    val uploadId = uploadSession.getId

    def writeToFile(context: TaskContext, iter: Iterator[T]) {
      val account_ = new AliyunAccount(accessId, accessKey)
      val odps_ = new Odps(account_)
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

  private def clusterSaveTable[T: ClassTag](tableName: String, projectName: String,
                                            rdd: RDD[T],
                                            transferUser: Any, isOverWrite: Boolean): Set[String] = {
    var savePartitionPathResult = Set[String]()
    logInfo("now in save =")
    val saveMcode = (index: Int, serializedSaveResults: Array[Byte]) => {
      val saveResults = SavePaths.parseFrom(serializedSaveResults)
      logInfo("save multi rename files num = " + saveResults.getSavePathItermCount())
      for (savePathIterm <- saveResults.getSavePathItermList().asScala) {
        val ddlLocationFile = savePathIterm.getDdlLocation()
        val ddlLocationDir = ddlLocationFile.substring(0, ddlLocationFile.lastIndexOf("/"))
        savePartitionPathResult += ddlLocationDir
      }
      logInfo("save multi ddl dir num = " + savePartitionPathResult.size)
    }

    val saveIdStr = CupidSession.get.saveId.getAndIncrement().toString
    val SaveTempDirAndCapFileResult: (String, String) =
      CupidTaskInterativeUtil.getSaveTempDirAndCapFile(
        CupidUtil.getEngineLookupName(),
        projectName,
        tableName,
        saveIdStr)
    val saveTempFileDir = SaveTempDirAndCapFileResult._1
    val saveTableTempCapFileName = SaveTempDirAndCapFileResult._2


    val (schema, columnsNames) = {
      var tableSchema = new util.ArrayList[String]
      var tableColumnsNames = ArrayBuffer[String]()
      CupidSession.get.odps.tables().
        get(projectName, tableName).
        getSchema().getColumns().asScala.zipWithIndex.foreach {
        case (column, index) =>
          if (column.getName() != null) {
            tableColumnsNames += column.getName()
          } else {
            throw new IOException("the column " + index + " do not have the column name")
          }

          tableSchema.add(column.getTypeInfo.getTypeName)
      }
      (tableSchema, tableColumnsNames.toArray)
    }


    def getTableSchema(): TableSchema = {
      val tableSchematmp = new TableSchema
      for (i <- 0 until schema.size()) {
        tableSchematmp.addColumn(new Column(columnsNames(i),
          TypeInfoParser.getTypeInfoFromTypeString(schema.get(i))))
      }
      tableSchematmp
    }

    val saveWcode: (TaskContext, Iterator[T]) => Array[Byte] = (context, it) => {
      def getTablePartDir(tableName: String, projectName: String, partStr: String): String = {
        var outPutKeyDir = projectName + "/" + tableName + "/"
        if (partStr != null) {
          outPutKeyDir = outPutKeyDir + partStr + "/"
        }
        outPutKeyDir
      }

      def getCols(tschema: TableSchema): Array[Column] = {
        tschema.getColumns().toArray(Array[Column]())
      }

      var savePaths = SavePaths.newBuilder()
      var savePathIterm = SavePathIterm.newBuilder()
      savePaths.setSaveTempCapFile(saveTableTempCapFileName)
      val tmpLocationPrefix = System.getenv("APP_VERSION") + "/save/" + saveIdStr + "_tmp" + "/"
      val ddlLocationPrefix = System.getenv("APP_VERSION") + "/save/" + saveIdStr + "/"

      val tschema = getTableSchema()
      val cols = getCols(tschema)
      val record = new ArrayRecord(cols)
      val transfer = transferUser
      try {
        if (transfer.isInstanceOf[(T, Record, TableSchema) => Unit]) {
          val tablePartDir = getTablePartDir(tableName, projectName, null)
          val taskPartitionFileName =
            System.getenv("META_LOOKUP_NAME") + "_" +
              context.stageId + "_" +
              context.partitionId

          val taskAttemptFileName =
            UUID.randomUUID() + "_" +
              taskPartitionFileName + "_" +
              context.taskAttemptId()

          val tableWriter = RuntimeContext.get()
            .createTableWriterByLabel(getSchemaStr(schema), saveTempFileDir +
              tmpLocationPrefix + tablePartDir + taskAttemptFileName, saveTableTempCapFileName)
          context.addTaskCompletionListener(_ => tableWriter.close)
          val userTransfer = transfer.asInstanceOf[(T, Record, TableSchema) => Unit]
          var i = 0
          while (it.hasNext) {
            val rec = it.next
            (0 until cols.length).foreach(record.set(_, null))
            userTransfer(rec, record, tschema)
            tableWriter.writeRecord(record)
          }
          tableWriter.close
          context.taskMetrics().outputMetrics.setRecordsWritten(tableWriter.recordsWrite())
          context.taskMetrics().outputMetrics.setBytesWritten(tableWriter.bytesWrite())
          val tmpLocation = saveTempFileDir + tmpLocationPrefix + tablePartDir + taskAttemptFileName
          val ddlLocation = saveTempFileDir +
            ddlLocationPrefix +
            tablePartDir +
            taskPartitionFileName

          savePathIterm.setTmpLocation(tmpLocation)
          savePathIterm.setDdlLocation(ddlLocation)
          savePaths.addSavePathIterm(savePathIterm.build())
        } else if (transfer.isInstanceOf[(T, Record, PartitionSpec, TableSchema) => Unit]) {
          var prePartSpectString: String = null
          var nowTaskAttemptFileName: String = null
          var nowTaskPartitionFileName: String = null
          var tableWriter: TableWriterAbstract = null
          var tablePartDir: String = null
          val userTransfer = transfer.asInstanceOf[(T, Record, PartitionSpec, TableSchema) => Unit]

          var bytesWrite: Long = 0
          var recordsWrite: Long = 0

          val saveMultiIter = new NextIterator[(String, Array[AnyRef])] {
            def getNext(): (String, Array[AnyRef]) = {
              if (it.hasNext) {
                val iterNext = it.next
                var parSpec = new PartitionSpec
                val newRecord = new ArrayRecord(cols)
                userTransfer(iterNext, newRecord, parSpec, tschema)
                if (parSpec.toString() == "") {
                  throw new Exception(
                    "use the saveToTableForMultiPartition interface must set the  PartitionSpec")
                }
                (parSpec.toString(), newRecord.toArray)
              } else {
                logInfo("save multiiter end")
                finished = true
                null.asInstanceOf[(String, Array[AnyRef])]
              }
            }

            def close() {
              logInfo("save multi end close")
            }
          }

          val caseInsensitiveOrdering = new Ordering[String] {
            override def compare(a: String, b: String) = a.toLowerCase.compare(b.toLowerCase)
          }

          val sorter = new ExternalSorter[String, Any, Any](
            context, None, None, Some(caseInsensitiveOrdering))

          sorter.insertAll(saveMultiIter.asInstanceOf[Iterator[Product2[String, Any]]])
          val sortIter = sorter.iterator

          def closeWriterAndAddPath() {
            if (tableWriter != null) {
              tableWriter.close
              bytesWrite += tableWriter.bytesWrite()
              recordsWrite += tableWriter.recordsWrite()
              val tmpLocation = saveTempFileDir +
                tmpLocationPrefix +
                tablePartDir +
                nowTaskAttemptFileName
              val ddlLocation = saveTempFileDir +
                ddlLocationPrefix +
                tablePartDir +
                nowTaskPartitionFileName
              savePathIterm.setTmpLocation(tmpLocation)
              savePathIterm.setDdlLocation(ddlLocation)
              savePaths.addSavePathIterm(savePathIterm.build())
            }
          }

          def changeOrInitWriter(sortIterPartKey: String) = {
            if (prePartSpectString == null || sortIterPartKey != prePartSpectString) {
              logInfo("now close pre writer sortIterPartKey=" + sortIterPartKey)
              closeWriterAndAddPath()
              logInfo("now write sortIterPartKey=" + sortIterPartKey)

              nowTaskPartitionFileName = System.getenv("META_LOOKUP_NAME") + "_" +
                context.stageId + "_" +
                context.partitionId
              nowTaskAttemptFileName = UUID.randomUUID() + "_" +
                nowTaskPartitionFileName + "_" +
                context.taskAttemptId()

              tablePartDir = getTablePartDir(tableName, projectName, sortIterPartKey)
              tableWriter = RuntimeContext.get()
                .createTableWriterByLabel(getSchemaStr(schema), saveTempFileDir +
                  tmpLocationPrefix + tablePartDir + nowTaskAttemptFileName, saveTableTempCapFileName)
              context.addTaskCompletionListener(_ => tableWriter.close)
              logInfo("saveTempFileDirMulti = " + saveTempFileDir +
                ",saveTableTempCapFileName= " + saveTableTempCapFileName)

              logInfo("now open writer over,sortIterPartKey=" + sortIterPartKey)
              prePartSpectString = sortIterPartKey
            }
          }

          while (sortIter.hasNext) {
            val sortNext = sortIter.next
            changeOrInitWriter(sortNext._1)
            val values = sortNext._2.asInstanceOf[Array[AnyRef]]
            record.set(values)
            tableWriter.writeRecord(record)
          }
          closeWriterAndAddPath()

          // update total write metrics
          context.taskMetrics().outputMetrics.setRecordsWritten(recordsWrite)
          context.taskMetrics().outputMetrics.setBytesWritten(bytesWrite)
        }
      } finally {
        logInfo("now save over," + "savePaths.size=" + savePaths.getSavePathItermCount())
      }
      val outputCommitCoordinator = SparkEnv.get.outputCommitCoordinator
      val taskAttemptNumber = context.attemptNumber()
      logInfo("now save over,then commit:" +
        "context.taskid=" + context.taskAttemptId() +
        "taskAttemptNumber=" + taskAttemptNumber)

      val canCommit =
        outputCommitCoordinator.canCommit(
          context.stageId,
          context.partitionId,
          taskAttemptNumber)

      if (canCommit) {
        logInfo("canCommit,savePaths iterms num = " + savePaths.build().getSavePathItermCount)
        val taskAttempId =
          context.stageId + "_" +
            context.partitionId + "_" +
            context.attemptNumber()

        RuntimeContext.get().commitTaskRenamePaths(savePaths.build(), taskAttempId.toString)
      } else {
        val message = context.taskAttemptId() +
          ": Not committed because the driver did not authorize commit"
        logInfo(message)
        throw new CommitDeniedException(
          message, context.stageId, context.partitionId, taskAttemptNumber)
      }

      savePaths.build().toByteArray
    }
    sc.runJob(rdd, saveWcode, saveMcode)
    logInfo("savePartitionPathResult.size = " + savePartitionPathResult.size)
    savePartitionPathResult

  }

  private def getSchemaStr(schema: util.ArrayList[String]) = {
    "|" + schema.asScala.mkString("|")
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
