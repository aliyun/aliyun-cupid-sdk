//scalastyle:off
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import java.io.IOException
import java.util.UUID

import com.aliyun.odps.`type`._
import com.aliyun.odps.cupid.runtime.{NextIterator, TableWriterAbstract}
import com.aliyun.odps.cupid.table.{CommitFileEntry, TableImplUtils}
import com.aliyun.odps.cupid.{CupidSession, ExceedMaxOpenFileException, ExceedMaxPartitionSpecException}
import com.aliyun.odps.data.{ArrayRecord, Record}
import com.aliyun.odps.{Column, PartitionSpec, TableSchema}
import org.apache.spark.executor.CommitDeniedException
import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.{Logging, SparkEnv, SparkException, TaskContext}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, Set}
import scala.reflect.ClassTag

class OdpsRDDActions[T: ClassTag](self: RDD[T])
  extends Serializable  with Logging {
  def save(
            tableName: String,
            projectName: String,
            transferUser: Any,
            isOverWrite: Boolean,
            partSpecOut: String = ""): Unit = self.withScope {

    var partSpecs = Set[String]()
    val saveMcode = (index: Int, executorResult: Array[String]) => {
      for (partSpec: String <- executorResult) {
        partSpecs += partSpec
      }
      logInfo(s"Final DDL partSpec count: ${partSpecs.size}")
    }
    val maxOpenFiles: Long = self.context.getConf.getLong("odps.cupid.open.file.max", 100000)
    val maxPartSpecNum: Long = self.context.getConf.getLong("odps.cupid.partition.spec.max", 1000)
    val numPartitions : Int = self.partitions.size
    logInfo(s"maxOpenFiles: ${maxOpenFiles}, " +
      s"maxPartSpecNums: ${maxPartSpecNum}, " +
      s"numPartitions: ${numPartitions}")

    val commitFileEntry: ArrayBuffer[CommitFileEntry] = new ArrayBuffer[CommitFileEntry]()
    val tableOutputHandle = TableImplUtils.getOrCreateOutputFormat.writeTable(
      projectName,
      tableName
    )

    val (schema, columnsNames) = {
      var tableSchema = new java.util.ArrayList[String]
      var tableColumnsNames = ArrayBuffer[String]()
      CupidSession.get.odps().tables().
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

    val saveWcode: (TaskContext, Iterator[T]) => Array[String] = (context, it) => {
      def getCols(tschema: TableSchema): Array[Column] = {
        tschema.getColumns().toArray(Array[Column]())
      }

      val tschema = getTableSchema()
      val cols = getCols(tschema)
      val record = new ArrayRecord(cols)
      val transfer = transferUser
      try {
        if (transfer.isInstanceOf[(T, Record, TableSchema) => Unit]) {
          val taskPartitionFileName =
            System.getenv("META_LOOKUP_NAME") + "_" +
              context.stageId + "_" +
              context.partitionId

          val taskAttemptFileName =
            UUID.randomUUID() + "_" +
              taskPartitionFileName + "_" +
              context.taskAttemptId()

          val tableWriter = TableImplUtils.getOrCreateOutputFormat.writeTableFile(
            tableOutputHandle, partSpecOut.trim.replaceAll("'", ""), taskAttemptFileName)
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
          context.taskMetrics().outputMetrics.foreach(_.setRecordsWritten(tableWriter.recordsWrite()))
          context.taskMetrics().outputMetrics.foreach(_.setBytesWritten(tableWriter.bytesWrite()))
          commitFileEntry.append(
            new CommitFileEntry(
              partSpecOut.trim.replaceAll("'", ""),
              taskAttemptFileName,
              taskPartitionFileName
            )
          )
        } else if (transfer.isInstanceOf[(T, Record, PartitionSpec, TableSchema) => Unit]) {
          var prePartSpectString: String = null
          var nowTaskAttemptFileName: String = null
          var nowTaskPartitionFileName: String = null
          var tableWriter: TableWriterAbstract = null
          var sortIterPartKey: String = null
          var tablePartDir: String = null
          val userTransfer = transfer.asInstanceOf[(T, Record, PartitionSpec, TableSchema) => Unit]

          var bytesWrite: Long = 0
          var recordsWrite: Long = 0
          var openFiles: Long = 0

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

          if (saveMultiIter.isInstanceOf[java.util.Iterator[_]]) {
            sorter.insertAll(saveMultiIter.asScala.asInstanceOf[Iterator[Product2[String, Any]]])
          } else {
            sorter.insertAll(saveMultiIter.asInstanceOf[Iterator[Product2[String, Any]]])
          }
          val sortIter = sorter.iterator
          def closeWriterAndAddPath() {
            if (tableWriter != null) {
              tableWriter.close
              bytesWrite += tableWriter.bytesWrite()
              recordsWrite += tableWriter.recordsWrite()
              commitFileEntry.append(
                new CommitFileEntry(
                  prePartSpectString.trim().replaceAll("'", ""),
                  nowTaskAttemptFileName,
                  nowTaskPartitionFileName
                )
              )
            }
          }

          def changeOrInitWriter() = {
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

              tableWriter = TableImplUtils.getOrCreateOutputFormat.writeTableFile(
                tableOutputHandle,
                sortIterPartKey.trim().replaceAll("'", ""),
                nowTaskAttemptFileName
              )
              openFiles = openFiles + 1

              val errMsg = s"NumPartitions: ${numPartitions}, " +
                s"maxPartSpecNum: ${maxPartSpecNum}, " +
                s"openFiles: ${openFiles}, " +
                s"maxOpenFiles: ${maxOpenFiles}, exceeded Max limit."

              // Single rdd partition exceeded max partition spec count
              if (openFiles > maxPartSpecNum) {
                logError(errMsg)
                throw new ExceedMaxPartitionSpecException(errMsg)
              }

              // Total open files across workers exceeded max open file count
              if (numPartitions * openFiles > maxOpenFiles) {
                logError(errMsg)
                throw new ExceedMaxOpenFileException(errMsg)
              }

              context.addTaskCompletionListener(_ => tableWriter.close)
              logInfo("now open writer over,sortIterPartKey=" + sortIterPartKey)
              prePartSpectString = sortIterPartKey
            }
          }

          while (sortIter.hasNext) {
            val sortNext = sortIter.next
            sortIterPartKey = sortNext._1
            changeOrInitWriter()
            val values = sortNext._2.asInstanceOf[Array[AnyRef]]
            record.set(values)
            tableWriter.writeRecord(record)
          }
          closeWriterAndAddPath()

          // update total write metrics
          context.taskMetrics().outputMetrics.foreach(_.setRecordsWritten(recordsWrite))
          context.taskMetrics().outputMetrics.foreach(_.setBytesWritten(bytesWrite))
        }
      } finally {
        logInfo("now save over," + "savePaths.size=" + commitFileEntry.length)
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
        logInfo("canCommit,savePaths iterms num = " + commitFileEntry.length)
        val taskAttempId =
          context.stageId + "_" +
            context.partitionId + "_" +
            context.attemptNumber()

        TableImplUtils.getOrCreateOutputFormat.commitTableFiles(
          tableOutputHandle,
          commitFileEntry.toArray
        )
      } else {
        val message = context.taskAttemptId() +
          ": Not committed because the driver did not authorize commit"
        logInfo(message)
        throw new CommitDeniedException(
          message, context.stageId, context.partitionId, taskAttemptNumber)
      }
      commitFileEntry.map(x => x.partSpec).toArray
    }

    try {
      self.context.runJob(self, saveWcode, saveMcode)
      TableImplUtils.getOrCreateOutputFormat.
        commitTable(tableOutputHandle, isOverWrite, partSpecs.toArray)
    }
    catch {
      case ex: SparkException =>
        if (ex.getCause.isInstanceOf[ExceedMaxOpenFileException] ||
          ex.getCause.isInstanceOf[ExceedMaxPartitionSpecException]) {
          logError(s"SaveToMultiPartition exceeded limit with error: ${ex.getMessage}")
          throw ex
        }
        else {
          throw ex
        }
    }
    finally {
      // Do output handle clean up
      TableImplUtils.getOrCreateOutputFormat.closeOutputHandle(tableOutputHandle)
    }
  }

  private def getSchemaStr(schema: java.util.ArrayList[String]) = {
    "|" + schema.asScala.mkString("|")
  }
}

object OdpsRDDActions {
  def fromRDD[T: ClassTag](rdd: RDD[T]): OdpsRDDActions[T] = {
    new OdpsRDDActions[T](rdd)
  }
}