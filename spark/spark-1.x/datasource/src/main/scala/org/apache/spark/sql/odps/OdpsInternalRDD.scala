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
package org.apache.spark.sql.odps

import java.io.EOFException

import com.aliyun.odps._
import com.aliyun.odps.`type`.{ArrayTypeInfo, DecimalTypeInfo, MapTypeInfo, TypeInfo}
import com.aliyun.odps.account.AliyunAccount
import com.aliyun.odps.cupid.CupidSession
import com.aliyun.odps.cupid.runtime.TableReaderIterator
import com.aliyun.odps.cupid.table.{TableImplUtils, _}
import com.aliyun.odps.data.{ArrayRecord, Binary, Char, Record, Varchar}
import com.aliyun.odps.tunnel.TableTunnel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.NextIterator
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class OdpsInternalRDDPartition(
                                val rddId: Int,
                                val tableInputHandle: TableInputHandle,
                                val inputSplit: InputSplit,
                                val idx: Int,
                                val splitFileStart: String,
                                val splitFileEnd: String,
                                val schemaSplitFileStart: String,
                                val schemaSplitFileEnd: String,
                                val splitTempDir: String,
                                val start: Long = -1L,
                                val count: Long = -1L)
  extends Partition {

  override def equals(other: Any): Boolean = other match {
    case partition: OdpsInternalRDDPartition => partition.idx == idx && partition.rddId == rddId
    case _ => false
  }

  override def hashCode(): Int = (41 * (41 + rddId) + idx)
  override val index: Int = idx
}

class OdpsInternalRDD(
                       @transient sc: SparkContext,
                       val accessId: String,
                       val accessKey: String,
                       val odpsUrl: String,
                       val project: String, // projectName
                       val table: String, // tableName
                       val columns: Array[String], // target fields Array[String]("id", "value")
                       val part: Array[String], // partitionFields Array[String]("pt1='1'", "pt2='2'")
                       val isLocal: Boolean, // local or cupid mode
                       val numPartition: Int, // parallelize parameter
                       val splitSize: Int = 0,
                       val partColNum: Int = 0)
  extends RDD[InternalRow](sc, Nil) {

  override def compute(theSplit: Partition, context: TaskContext): Iterator[InternalRow] = {
    val iter: Iterator[InternalRow] =
      if (isLocal) {
        new LocalIter(theSplit, context)
      } else {
        new CupidIter(theSplit.asInstanceOf[OdpsInternalRDDPartition], context)
      }
    new InterruptibleIterator[InternalRow](context, iter)
  }

  override def getPartitions: Array[Partition] = {
    if (isLocal) {
      getPartitionInfoFromTunnel()
    } else {
      var odpsPartitions = ArrayBuffer[OdpsInternalRDDPartition]()
      if(part.length == 0 && partColNum != 0) {
        odpsPartitions += new OdpsInternalRDDPartition(
          this.id,
          null,
          null,
          0,
          "",
          "",
          "",
          "",
          "",
          0,
          0)
      } else {
        val partFields = if (part.length == 0) {
          Array[String]()
        } else {
          val partitionSpec = new PartitionSpec(part(0))
          partitionSpec.keys().asScala.toArray
        }

        val tableInputInfos : Array[TableInputInfo] = TableInputInfo.generateTableInputInfos(
          project,
          table,
          columns.filter(!partFields.contains(_)),
          part
        )

        val tableInputHandle : TableInputHandle =
          TableImplUtils.getOrCreateInputFormat.splitTables(
            tableInputInfos, splitSize, numPartition)

        val inputSplits : Array[InputSplit] =
          TableImplUtils.getOrCreateInputFormat.getSplits(tableInputHandle)

        logInfo("project=" + project +
          ",table=" + table +
          ",partitionsize=" +
          inputSplits.length)

        inputSplits.foreach(inputSplit => {
          odpsPartitions += new OdpsInternalRDDPartition(
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
      }
      odpsPartitions.toArray
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
        count = Math.min(
          sc.env.conf.getInt("spark.hadoop.odps.spark.local.partition.amt", count), count)
        new OdpsInternalRDDPartition(
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
    }.filter(p => p.count > 0)
      .map(_.asInstanceOf[Partition])
    // remove the last count==0 to prevent exceptions from reading odps table.
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

  class LocalIter(theSplit: Partition, context: TaskContext) extends NextIterator[InternalRow] {
    val split = theSplit.asInstanceOf[OdpsInternalRDDPartition]
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

    lazy val indexOdpsDataType: ArrayBuffer[TypeInfo] = {
      val odpsSchema: TableSchema = downloadSession.getSchema
      val columnsInfo = odpsSchema.getColumns.asScala
      val partitionColumnsInfo = odpsSchema.getPartitionColumns.asScala
      val indexOdpsDataType = new ArrayBuffer[TypeInfo]()

      columns.foreach(column => {
        if (columnsInfo.map(_.getName).contains(column)) {
          val typeInfo = odpsSchema.getColumn(column).getTypeInfo
          indexOdpsDataType.append(typeInfo)
        }

        if (partitionColumnsInfo.map(_.getName).contains(column)) {
          val typeInfo = odpsSchema.getPartitionColumn(column).getTypeInfo
          indexOdpsDataType.append(typeInfo)
        }
      })

      indexOdpsDataType
    }

    lazy val dataTypes: Seq[DataType] = {
      indexOdpsDataType.map(typeInfo => OdpsInternalRDD.OdpsTypeConverter(typeInfo))
    }

    def transferOdpsData(index: Int, v: Object): Any = {
      if (v == null) {
        null
      } else {
        dataTransfers(index)(v)
      }
    }

    override def getNext(): InternalRow = {
      val ret = new SpecificMutableRow(dataTypes)
      try {
        val r = reader.read().asInstanceOf[ArrayRecord]
        if (r != null) {
          columns.zipWithIndex.foreach(target => {
            ret.update(target._2, transferOdpsData(target._2, r.get(target._2)))
          })
        } else {
          finished = true
        }
      } catch {
        case eof: EOFException =>
          finished = true
      }
      ret
    }

    lazy val dataTransfers: Seq[Object => Any] = {
      indexOdpsDataType.map(typeInfo => OdpsInternalRDD.odpsData2SparkData(typeInfo))
    }

    lazy val reusedRow = new SpecificMutableRow(dataTypes)

    override def close() {
      try {
        reader.close()
      } catch {
        case e: Exception => logWarning("Exception in RecordReader.close()", e)
      }
    }
  }

  class CupidIter(split: OdpsInternalRDDPartition, context: TaskContext)
    extends NextIterator[InternalRow] {
    logInfo("rddId=" + id +
      ",tableInputHandleId=" + split.tableInputHandle.getTableInputHandleId +
      ",index=" + split.index +
      ",splitFileStart=" + split.splitFileStart +
      ",splitFileEnd=" + split.splitFileEnd +
      ",schemaSplitFileStart=" + split.schemaSplitFileStart +
      ",schemaSplitFileEnd=" + split.schemaSplitFileEnd +
      ",splitTempDir=" + split.splitTempDir)


    var schema: TableSchema = _
    var it: TableReaderIterator[Record] = _

    lazy val indexOdpsDataType: ArrayBuffer[TypeInfo] = {
      val odpsSchema: TableSchema = schema
      val columnsInfo = odpsSchema.getColumns.asScala
      val partitionColumnsInfo = odpsSchema.getPartitionColumns.asScala
      val indexOdpsDataType = new ArrayBuffer[TypeInfo]()

      columns.foreach(column => {
        if (columnsInfo.map(_.getName).contains(column)) {
          val typeInfo = odpsSchema.getColumn(column).getTypeInfo
          indexOdpsDataType.append(typeInfo)
        }

        if (partitionColumnsInfo.map(_.getName).contains(column)) {
          val typeInfo = odpsSchema.getPartitionColumn(column).getTypeInfo
          indexOdpsDataType.append(typeInfo)
        }
      })

      indexOdpsDataType
    }

    lazy val dataTypes: Seq[DataType] = {
      indexOdpsDataType.map(typeInfo => OdpsInternalRDD.OdpsTypeConverter(typeInfo))
    }

    lazy val dataTransfers: Seq[Object => Any] = {
      indexOdpsDataType.map(typeInfo => OdpsInternalRDD.odpsData2SparkData(typeInfo))
    }

    lazy val reusedRow = new SpecificMutableRow(dataTypes)

    def transferOdpsData(index: Int, v: Object): Any = {
      if (v == null) {
        null
      } else {
        dataTransfers(index)(v)
      }
    }

    if (split.count != 0) {
      val tableReader: TableReader =
        TableImplUtils.getOrCreateInputFormat.readSplit(split.tableInputHandle, split.inputSplit)

      schema = tableReader.getSchema
      it = tableReader.getIterator
      context.addTaskCompletionListener(_ =>
        it.asInstanceOf[TableReaderIterator[Record]].closeIfNeeded())
    }

    override def getNext(): InternalRow = {
      if (it == null) {
        finished = true
        null.asInstanceOf[InternalRow]
      } else {
        if (it.hasNext) {
          finished = false
          for (i <- 0 until columns.length) {
            reusedRow.setNullAt(i)
          }
          val r = it.next().asInstanceOf[ArrayRecord]

          if (r != null) {
            for (i <- 0 until columns.length) {
              reusedRow.update(i, transferOdpsData(i, r.get(i)))
            }
          }
          reusedRow
        } else {
          finished = true
          context.taskMetrics().inputMetrics.map(_.incRecordsRead(it.recordsRead()))
          context.taskMetrics().inputMetrics.map(_.incBytesRead(it.bytesRead()))
          null.asInstanceOf[InternalRow]
        }
      }
    }

    override def close() {
    }
  }

}


object OdpsInternalRDD {
  def OdpsTypeConverter(typeInfo: TypeInfo): DataType = {
    OdpsMetastoreCatalog.typeInfo2Type(typeInfo)
  }

  def odpsData2SparkData(t: TypeInfo): Object => Any = {
    t.getOdpsType match {
      case OdpsType.BOOLEAN => (v: Object) => v.asInstanceOf[java.lang.Boolean]
      case OdpsType.DOUBLE => (v: Object) => v.asInstanceOf[java.lang.Double]
      case OdpsType.BIGINT => (v: Object) => v.asInstanceOf[java.lang.Long]
      case OdpsType.DATETIME => (v: Object) => v.asInstanceOf[java.util.Date].getTime / 1000 * 1000000
      case OdpsType.STRING => (v: Object) => {
        if (v.isInstanceOf[String]) {
          UTF8String.fromString(v.asInstanceOf[String])
        }
        else {
          UTF8String.fromBytes(v.asInstanceOf[Array[Byte]])
        }
      }
      case OdpsType.DECIMAL => (v: Object) => {
        val ti = t.asInstanceOf[DecimalTypeInfo]
        (new Decimal).set(v.asInstanceOf[java.math.BigDecimal], ti.getPrecision, ti.getScale)
      }
      case OdpsType.VARCHAR => (v: Object) => {
        val varchar = v.asInstanceOf[Varchar]
        UTF8String.fromString(varchar.getValue.substring(0, varchar.length()))
      }
      case OdpsType.CHAR => (v: Object) => {
        val char = v.asInstanceOf[Char]
        UTF8String.fromString(char.getValue.substring(0, char.length()))
      }
      case OdpsType.DATE => (v: Object) =>
        ((v.asInstanceOf[java.sql.Date].getTime +
          (3600 * 24 * 1000 - 1)) / (3600 * 24 * 1000)).toInt
      case OdpsType.TIMESTAMP => (v: Object) => v.asInstanceOf[java.sql.Timestamp].getTime * 1000
      case OdpsType.FLOAT => (v: Object) => v.asInstanceOf[java.lang.Float]
      case OdpsType.INT => (v: Object) => v.asInstanceOf[java.lang.Integer]
      case OdpsType.SMALLINT => (v: Object) => v.asInstanceOf[java.lang.Short]
      case OdpsType.TINYINT => (v: Object) => v.asInstanceOf[java.lang.Byte]
      case OdpsType.ARRAY => (v: Object) => {
        val array = v.asInstanceOf[java.util.ArrayList[Object]]
        new GenericArrayData(array.toArray().
          map(odpsData2SparkData(t.asInstanceOf[ArrayTypeInfo].getElementTypeInfo)(_)))
      }
      case OdpsType.BINARY => (v: Object) => v.asInstanceOf[Binary].data()
      case OdpsType.MAP => (v: Object) => {
        val m = v.asInstanceOf[java.util.HashMap[Object, Object]]
        val keyArray = m.keySet().toArray()
        new ArrayBasedMapData(
          new GenericArrayData(keyArray.
            map(odpsData2SparkData(t.asInstanceOf[MapTypeInfo].getKeyTypeInfo)(_))),
          new GenericArrayData(keyArray.map(m.get(_)).
            map(odpsData2SparkData(t.asInstanceOf[MapTypeInfo].getValueTypeInfo)(_)))
        )
      }
    }
  }
}
