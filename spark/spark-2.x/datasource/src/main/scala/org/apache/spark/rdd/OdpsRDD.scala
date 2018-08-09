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

package org.apache.spark.rdd

import com.aliyun.odps.cupid.runtime.{RuntimeContext, TableReaderIterator}
import org.apache.spark.executor.DataReadMethod

import scala.reflect.ClassTag
import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.util.NextIterator
import org.apache.spark.InterruptibleIterator
import com.aliyun.odps.data.Record
import com.aliyun.odps.TableSchema
import com.aliyun.odps.cupid.requestcupid.PartitionSizeUtil
import java.io.EOFException

import com.aliyun.odps.account.AliyunAccount
import com.aliyun.odps.Odps
import com.aliyun.odps.tunnel.TableTunnel
import com.aliyun.odps.PartitionSpec
import com.aliyun.odps.cupid.CupidSession
import com.aliyun.odps.cupid.table.TableImplUtils
import com.aliyun.odps.cupid.table.{InputSplit, TableInputHandle, TableInputInfo, TableReader}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

/**
 * A Spark split class that wraps around a Hadoop InputSplit.
 */
class OdpsPartition(rddId: Int,
                    val tableInputHandle: TableInputHandle,
                    val inputSplit: InputSplit,
                    idx: Int,
                    val splitFileStart: String,
                    val splitFileEnd: String,
                    val schemaSplitFileStart: String,
                    val schemaSplitFileEnd: String,
                    val splitTempDir: String,
                    val start: Long = -1L,
                    val count: Long = -1L)
  extends Partition {

  override def hashCode(): Int = (41 * (41 + rddId) + idx).toInt

  override val index: Int = idx

}

class OdpsRDD[W: ClassTag](
                            @transient sc: SparkContext,
                            accessId: String,
                            accessKey: String,
                            odpsUrl: String,
                            val project: String,
                            val table: String,
                            val part: Array[String],
                            val numPartition: Int,
                            transfer: (Record, TableSchema) => W,
                            isLocal: Boolean, columns:Array[String], val splitSize: Int = 0) extends RDD[W](sc, Nil) {

  def this(sc: SparkContext, accessId: String, accessKey: String,
           odpsUrl: String, project: String, table: String,
           numPartition: Int,
           transfer: (Record, TableSchema) => W,
           isLocal: Boolean) {

    this(sc, accessId, accessKey, odpsUrl, project, table, Array[String](), numPartition, transfer, isLocal,  Array[String]())
  }

  override def compute(theSplit: Partition, context: TaskContext): Iterator[W] = {
    val iter: Iterator[W] = if (isLocal) new LocalIter(theSplit, context) else new CupidIter(theSplit.asInstanceOf[OdpsPartition], context)
    new InterruptibleIterator[W](context, iter)
  }

  override def getPartitions: Array[Partition] = {
    if (isLocal)
      getPartitionInfoFromTunnel()
    else {
      val tableInputInfos : Array[TableInputInfo] = TableInputInfo.generateTableInputInfos(
        project,
        table,
        columns,
        part
      )

      val tableInputHandle : TableInputHandle =
        TableImplUtils.getOrCreateInputFormat.splitTables(
          tableInputInfos, splitSize, numPartition)

      val inputSplits : Array[InputSplit] =
        TableImplUtils.getOrCreateInputFormat.getSplits(tableInputHandle)

      logInfo("project=" + project + ",table=" + table + ",partitionsize=" + inputSplits.length)

      var odpsPartitons = ArrayBuffer[OdpsPartition]()
      inputSplits.foreach(inputSplit => {
        odpsPartitons += new OdpsPartition(
          id,
          tableInputHandle,
          inputSplit,
          inputSplit.getSplitIndexId,
          inputSplit.getSplitFileStart.toString,
          inputSplit.getSplitFileEnd.toString,
          inputSplit.getSchemaFileStart.toString,
          inputSplit.getSchemaFileEnd.toString,
          "")
      })
      odpsPartitons.toArray
    }
  }


  def getPartitionInfoFromTunnel(): Array[Partition] = {
    var ret = null.asInstanceOf[Array[Partition]]
    val odps = CupidSession.get().odps()
    odps.setDefaultProject(project)
    odps.setEndpoint(odpsUrl)
    val tunnel = new TableTunnel(odps)
    val downloadSession = part.length match {
      case 0 => tunnel.createDownloadSession(project, table)
      case _ =>
        // hack now ,local need support multi partition
        tunnel.createDownloadSession(project, table, new PartitionSpec(part(0)))
    }
    val downloadCount = downloadSession.getRecordCount
    logDebug("Odps project " + project + " table " + table + " with partition "
      + part + " contain " + downloadCount + " line data.")
    val numPartition_ = math.min(math.max(1, numPartition),
      (if (downloadCount > Int.MaxValue) Int.MaxValue else downloadCount.toInt))
    val range = getRanges(downloadCount, 0, numPartition_)
    ret = Array.tabulate(numPartition_) {
      idx =>
        val (start, end) = range(idx)
        var count = (end - start + 1).toInt
        count = Math.min(sc.env.conf.getInt("odps.spark.local.partition.amt", count), count)
        new OdpsPartition(
          this.id,
          null,
          null,
          idx,
          "",
          "",
          "",
          "",
          "",
          start,
          count)
    }.filter(p => p.count > 0) //remove the last count==0 to prevent exceptions from reading odps table.
      .map(_.asInstanceOf[Partition])
    ret
  }

  def getRanges(max: Long, min: Long, numRanges: Int): Array[(Long, Long)] = {
    val span = max - min + 1
    val initSize = span / numRanges
    val sizes = Array.fill(numRanges)(initSize)
    val remainder = span - numRanges * initSize
    for (i <- 0 until remainder.toInt) {
      sizes(i) += 1
    }
    assert(sizes.reduce(_ + _) == span)
    val ranges = ArrayBuffer.empty[(Long, Long)]
    var start = min
    sizes.filter(_ > 0).foreach { size =>
      val end = start + size - 1
      ranges += Tuple2(start, end)
      start = end + 1
    }
    assert(start == max + 1)
    ranges.toArray
  }

  class LocalIter(theSplit: Partition, context: TaskContext) extends NextIterator[W] {
    val split = theSplit.asInstanceOf[OdpsPartition]
    val odps = CupidSession.get().odps()
    odps.setDefaultProject(project)
    odps.setEndpoint(odpsUrl)
    val tunnel = new TableTunnel(odps)
    val downloadSession = part.length match {
      case 0 => tunnel.createDownloadSession(project, table)
      case _ =>
        // hack now ,local need support multi partition
        tunnel.createDownloadSession(project, table, new PartitionSpec(part(0)))
    }
    val reader = downloadSession.openRecordReader(split.start, split.count)
    context.addTaskCompletionListener {
      _ => closeIfNeeded()
    }

    override def getNext() = {
      var ret = null.asInstanceOf[W]
      try {
        val r = reader.read()
        if (r != null) {
          ret = transfer(r, downloadSession.getSchema)
        } else {
          finished = true
        }
      } catch {
        case eof: EOFException =>
          finished = true
      }
      ret
    }

    override def close() {
      try {
        reader.close()
      } catch {
        case e: Exception => logWarning("Exception in RecordReader.close()", e)
      }
    }
  }

  class CupidIter(split: OdpsPartition, context: TaskContext) extends NextIterator[W] {
    logInfo("rddId=" + id +
      ",tableInputHandleId=" + split.tableInputHandle.getTableInputHandleId +
      ",index=" + split.index +
      ",splitFileStart=" +split.splitFileStart +
      ",splitFileEnd=" + split.splitFileEnd +
      ",schemaSplitFileStart=" + split.schemaSplitFileStart +
      ",schemaSplitFileEnd=" + split.schemaSplitFileEnd +
      ",splitTempDir=" + split.splitTempDir)
    val inputMetrics = context.taskMetrics.inputMetrics

    val tableReader: TableReader =
      TableImplUtils.getOrCreateInputFormat.readSplit(split.tableInputHandle, split.inputSplit)
    val (schema, it) = (tableReader.getSchema, tableReader.getIterator)
    context.addTaskCompletionListener(_ => it.asInstanceOf[TableReaderIterator[Record]].closeIfNeeded())

    override def getNext(): W = {
      if (it.hasNext) {
        finished = false
        transfer(it.next, schema)
      } else {
        finished = true
        inputMetrics.incRecordsRead(it.recordsRead())
        inputMetrics.incBytesRead(it.bytesRead())
        null.asInstanceOf[W]
      }
    }

    override def close() {
    }
  }

}

