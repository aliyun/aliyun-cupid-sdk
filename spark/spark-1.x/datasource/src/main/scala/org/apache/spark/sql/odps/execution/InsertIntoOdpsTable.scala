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
package org.apache.spark.sql.odps.execution

import java.math.BigDecimal

import com.aliyun.odps._
import com.aliyun.odps.`type`._
import com.aliyun.odps.account.AliyunAccount
import com.aliyun.odps.data.{Binary, Char, Record, SimpleStruct, Varchar}
import com.aliyun.odps.tunnel.TableTunnel
import org.apache.spark.rdd.{OdpsRDDActions, RDD}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.odps._
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.sql.{AnalysisException, Row, SQLContext}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{SparkException, TaskContext}

import scala.collection.JavaConverters._


case class InsertIntoOdpsTable(relation: OdpsRelation,
                               partition: Map[String, Option[String]],
                               query: LogicalPlan,
                               overwrite: Boolean,
                               ifNotExists: Boolean) extends RunnableCommand {

  val table = relation.table

  val isPartitionedTable = table.partitionColumns.length match {
    case 0 => false
    case _ => true
  }

  var sparkSchema: Array[DataType] = null
  var recordTransfers: Array[Object => Any] = null

  def transfer(r: InternalRow, record: Record, schema: TableSchema): Record = {
    if (sparkSchema == null) {
      sparkSchema = schema.getColumns.asScala.map(col => OdpsInternalRDD.OdpsTypeConverter(col.getTypeInfo)).toArray
    }
    if (recordTransfers == null) {
      recordTransfers = schema.getColumns.asScala.map(col => sparkData2OdpsData(col.getTypeInfo)).toArray
    }
    val len = schema.getColumns.size()
    for (i <- 0 until len) {
      record.set(i, if (r.isNullAt(i)) {
        null
      } else {
        val v = r.get(i, sparkSchema(i))
        recordTransfers(i)(v)
      })
    }
    record
  }

  def sparkData2OdpsData(t: TypeInfo): Object => Any = {
    t.getOdpsType match {
      case OdpsType.BOOLEAN => v: Object => v.asInstanceOf[Boolean]
      case OdpsType.DOUBLE => v: Object => v.asInstanceOf[Double]
      case OdpsType.BIGINT => v: Object => v.asInstanceOf[Long]
      case OdpsType.DATETIME => v: Object => new java.util.Date(v.asInstanceOf[Long] / 1000)
      case OdpsType.STRING => v: Object => v.asInstanceOf[UTF8String].toString
      case OdpsType.DECIMAL => v: Object => {
        val ti = t.asInstanceOf[DecimalTypeInfo]
        new BigDecimal(v.asInstanceOf[Decimal].toString).setScale(ti.getScale)
      }
      case OdpsType.VARCHAR => v: Object => {
        val ti = t.asInstanceOf[VarcharTypeInfo]
        new Varchar(v.asInstanceOf[UTF8String].toString, ti.getLength)
      }
      case OdpsType.CHAR => v: Object => {
        val ti = t.asInstanceOf[CharTypeInfo]
        new Char(v.asInstanceOf[UTF8String].toString, ti.getLength)
      }
      case OdpsType.DATE => v: Object => {
        new java.sql.Date(v.asInstanceOf[Int].toLong * (3600 * 24 * 1000))
      }
      case OdpsType.TIMESTAMP => v: Object => new java.sql.Timestamp(v.asInstanceOf[Long] / 1000)
      case OdpsType.FLOAT => v: Object => v.asInstanceOf[java.lang.Float]
      case OdpsType.INT => v: Object => v.asInstanceOf[java.lang.Integer]
      case OdpsType.SMALLINT => v: Object => v.asInstanceOf[java.lang.Short]
      case OdpsType.TINYINT => v: Object => v.asInstanceOf[java.lang.Byte]
      case OdpsType.ARRAY => v: Object => {
        val ti = t.asInstanceOf[ArrayTypeInfo]
        v.asInstanceOf[org.apache.spark.sql.catalyst.expressions.UnsafeArrayData]
          .toArray[Object](OdpsMetastoreCatalog.typeInfo2Type(ti.getElementTypeInfo))
          .map(e => sparkData2OdpsData(ti.getElementTypeInfo)(e)).toList.asJava
      }
      case OdpsType.BINARY => v: Object => new Binary(v.asInstanceOf[Array[Byte]])
      case OdpsType.MAP => v: Object => {
        val ti = t.asInstanceOf[MapTypeInfo]
        val m = new java.util.HashMap[Object, Object]
        val mapData = v.asInstanceOf[org.apache.spark.sql.catalyst.expressions.UnsafeMapData]
        mapData.keyArray.toArray[Object](OdpsMetastoreCatalog.typeInfo2Type(ti.getKeyTypeInfo))
          .zip(
            mapData.valueArray.toArray[Object](
              OdpsMetastoreCatalog.typeInfo2Type(ti.getValueTypeInfo)))
          .foreach(p => m.put(sparkData2OdpsData(ti.getKeyTypeInfo)(p._1.asInstanceOf[Object]).asInstanceOf[Object],
            sparkData2OdpsData(ti.getValueTypeInfo)(p._2.asInstanceOf[Object]).asInstanceOf[Object])
          )
        m
      }
      case OdpsType.STRUCT => v: Object => {
        val ti = t.asInstanceOf[StructTypeInfo]
        val r = v.asInstanceOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow]
        val l = (0 to r.numFields - 1).zip(ti.getFieldTypeInfos.toArray()).map(p =>
          sparkData2OdpsData(p._2.asInstanceOf[TypeInfo])(r.get(p._1,
            OdpsMetastoreCatalog.typeInfo2Type(p._2.asInstanceOf[TypeInfo]))).asInstanceOf[AnyRef]
        ).toList.asJava
        new SimpleStruct(ti, l)
      }
    }
  }

  def dynamicTransfer(providedPart: Map[String, String], partitionNames: Seq[String])(
    t: InternalRow,
    record: Record,
    part: PartitionSpec,
    schema: TableSchema): Record = {

    val providedSize = providedPart.size

    transfer(t, record, schema)

    val strDataType = OdpsInternalRDD.OdpsTypeConverter(
      TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.STRING))

    for (i <- (0 until partitionNames.size)) {
      val name = partitionNames(i)
      if (i < providedSize) {
        if (providedPart.get(name) == null) {
          throw new IllegalArgumentException("Invalid partition spec.")
        } else {
          part.set(name, providedPart.get(name).get)
        }
      } else {
        val index = schema.getColumns.size() + i - providedSize
        part.set(name, if (t.isNullAt(index)) {
          throw new IllegalArgumentException("Invalid partition spec.")
        } else {
          t.get(index, strDataType).asInstanceOf[UTF8String].toString
        })
      }
    }
    record
  }

  override def run(sqlContext: SQLContext): Seq[Row] = {

    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration

    val numDynamicPartitions = partition.values.count(_.isEmpty)
    val numStaticPartitions = partition.values.count(_.nonEmpty)
    val partitionSpec = partition.map {
      case (key, Some(value)) => key -> value
      case (key, None) => key -> ""
    }

    // All partition column names in the format of "<column name 1>/<column name 2>/..."
    val partitionColumns = table.partitionColumns.map(_.name).mkString("/")
    val partitionColumnNames = Option(partitionColumns.trim match {
      case "" => null
      case _ => partitionColumns.trim
    }
    ).map(_.split("/")).getOrElse(Array.empty)

    if (partitionColumnNames.toSet.size != partition.keySet.size ||
      partitionColumnNames.toSet != partition.keySet) {
      throw new SparkException(
        s"""Requested partitioning does not match the ${table.name} table:
           |Requested partitions: ${partition.keys.mkString(",")}
           |Table partitions: ${table.partitionSchema.map(_.name).mkString(",")}""".stripMargin)
    }


    // Validate partition spec if there exist any dynamic partitions
    if (numDynamicPartitions > 0) {
      // Report error if dynamic partitioning is not enabled
      if (!hadoopConf.get("odps.exec.dynamic.partition", "true").toBoolean) {
        throw new SparkException("Dynamic partition is disabled. Either enable it by setting "
          + "odps.exec.dynamic.partition=true or specify partition column values")
      }

      // Report error if dynamic partition strict mode is on but no static partition is found
      if (numStaticPartitions == 0 &&
        hadoopConf.get("odps.exec.dynamic.partition.mode", "strict").equalsIgnoreCase("strict")) {
        throw new SparkException("Dynamic partition strict mode requires at least one static " +
          "partition column. To turn this off set odps.exec.dynamic.partition.mode=nonstrict")
      }

      // Report error if any static partition appears after a dynamic partition
      val isDynamic = partitionColumnNames.map(partitionSpec(_).isEmpty)
      if (isDynamic.init.zip(isDynamic.tail).contains((true, false))) {
        throw new AnalysisException("Dynamic partition cannot be the parent of a static partition")
      }
    }

    val isDynamicPartition = if (numDynamicPartitions > 0) true else false

    if (!sqlContext.sparkContext.isLocal) {
      val defaultTransfer = isDynamicPartition match {
        case true => sqlContext.sparkContext.clean(dynamicTransfer(
          partition.filter(_._2.nonEmpty).map(entry => (entry._1, entry._2.get)),
          table.partitionColumns.map(_.name)) _)
        case false => sqlContext.sparkContext.clean(transfer _)
      }

      val partSpecOut = partitionSpec.map(entry => entry._1 + "=" + entry._2).mkString(",")
      // save will include ddl action
      val rdd = sqlContext.executePlan(query).toRdd
      val odpsRddActions = OdpsRDDActions.fromRDD(rdd)
      odpsRddActions.save(table.name, table.database, defaultTransfer, overwrite, partSpecOut)

      // Invalidate the cache.
      sqlContext.catalog.refreshTable(TableIdentifier(table.name, Some(table.database)))
    } else {
      if (overwrite == true) {
        logError("overwrite mode is not supported by local mode")
      }
      tunnelSaveTable(
        hadoopConf.get("odps.access.id"),
        hadoopConf.get("odps.access.key"),
        hadoopConf.get("odps.end.point"),
        table.database,
        table.name,
        sqlContext.executePlan(query).toRdd,
        sqlContext
      )
    }

    Seq.empty[Row]
  }

  private def tunnelSaveTable(
                               accessId: String,
                               accessKey: String,
                               odpsUrl: String,
                               project: String,
                               tableName: String,
                               rdd: RDD[InternalRow],
                               sqlContext: SQLContext): Unit = {

    def transfer(t: InternalRow, record: Record, scheme: TableSchema): Record = {
      // tranfer from InternalRow into Record

      scheme.getColumns.asScala.zipWithIndex.foreach(target => {
        record.set(target._2, if (t.isNullAt(target._2)) {
          null
        } else {
          if (target._1.getType == OdpsType.STRING) {
            t.get(target._2, OdpsInternalRDD.OdpsTypeConverter(target._1.getTypeInfo)).
              asInstanceOf[UTF8String].toString
          } else {
            t.get(target._2, OdpsInternalRDD.OdpsTypeConverter(target._1.getTypeInfo))
          }
        })
      })
      record
    }

    val transferFunc = sqlContext.sparkContext.clean(transfer _)

    val account = new AliyunAccount(accessId, accessKey)
    val odps = new Odps(account)

    odps.setDefaultProject(project)
    odps.setEndpoint(odpsUrl)

    val partitionSpecString = isPartitionedTable match {
      case false => ""
      case true =>
        partition.keys.map(k => s"$k=${partition(k).get}").toArray.mkString(",")
    }

    val catalog = sqlContext.catalog.asInstanceOf[OdpsMetastoreCatalog]

    val allPartitionSpecString: Array[String] = isPartitionedTable match {
      case false => Array[String]()
      case true => {
        val tableIdent = TableIdentifier(tableName, Some(project))
        val qualifiedTableName = catalog.getQualifiedTableName(tableIdent)
        val table = catalog.getTable(qualifiedTableName.database, qualifiedTableName.name)

        val qualifiedTable = OdpsRelation(
          table,
          table.dataSchema.asNullable.toAttributes,
          table.partitionSchema.asNullable.toAttributes
        )
        OdpsMetastoreCatalog.getPartitions(
          qualifiedTable,
          Nil
        )
      }
    }

    if (isPartitionedTable) {
      if (allPartitionSpecString.contains(partitionSpecString)) {
        // Do Nothing
      } else {
        // Create the partition
        catalog.createPartitions(
          project,
          tableName,
          Seq(partition.keys.map(k => k -> partition(k).get).toMap),
          ignoreIfExists = true
        )
      }
    }

    val tunnel = new TableTunnel(odps)
    val uploadSession = isPartitionedTable match {
      case false => tunnel.createUploadSession(project, tableName)
      case true =>
        tunnel.createUploadSession(project, tableName, new PartitionSpec(partitionSpecString))
    }

    logInfo("Odps upload session status is: " + uploadSession.getStatus.toString)
    val uploadId = uploadSession.getId

    def writeToFile(context: TaskContext, iter: Iterator[InternalRow]) {
      val account_ = new AliyunAccount(accessId, accessKey)
      val odps_ = new Odps(account_)
      odps_.setDefaultProject(project)
      odps_.setEndpoint(odpsUrl)
      val tunnel_ = new TableTunnel(odps_)

      val uploadSession_ = isPartitionedTable match {
        case false => tunnel_.getUploadSession(project, tableName, uploadId)
        case true =>
          val partitionSpec_ = new PartitionSpec(partitionSpecString)
          tunnel_.getUploadSession(project, tableName, partitionSpec_, uploadId)
      }

      val writer = uploadSession_.openRecordWriter(context.partitionId)
      while (iter.hasNext) {
        val value = iter.next()
        logDebug("context id: " + context.partitionId + " write: " + value)
        writer.write(transferFunc(value, uploadSession_.newRecord(), uploadSession_.getSchema))
      }
      logDebug("ready context id: " + context.partitionId)
      writer.close()
      logDebug("finish context id: " + context.partitionId)
    }

    sqlContext.sparkContext.runJob(rdd, writeToFile _)
    val arr = Array.tabulate(rdd.partitions.length)(l => Long.box(l))
    uploadSession.commit(arr)
  }
}
