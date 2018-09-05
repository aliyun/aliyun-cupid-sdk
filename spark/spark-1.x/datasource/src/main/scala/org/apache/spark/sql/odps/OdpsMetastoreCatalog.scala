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

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import javax.annotation.Nullable

import com.aliyun.odps._
import com.aliyun.odps.`type`._
import com.aliyun.odps.cupid.CupidSession
import com.aliyun.odps.task.SQLTask
import com.google.common.base.Objects
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.{Catalog, MultiInstanceRelation, NoSuchTableException}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BoundReference, Cast, Expression, InterpretedPredicate, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics, Subquery}
import org.apache.spark.sql.catalyst.{CatalystConf, InternalRow, TableIdentifier}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.collection.mutable

case class QualifiedTableName(database: String, name: String)

case class OdpsColumn(name: String, @Nullable odpsType: String, comment: String)

case class OdpsTable(
             specifiedDatabase: Option[String],
             name: String,
             dataColumns: Seq[OdpsColumn],
             partitionColumns: Seq[OdpsColumn],
             schema: StructType) {

  def database: String = specifiedDatabase.getOrElse("")

  def isPartitioned: Boolean = partitionColumns.nonEmpty

  def qualifiedName: String = s"$database.$name"

  def partitionSchema: StructType = {
    val partitionFileds = schema.takeRight(partitionColumns.length)
    assert(partitionFileds.map(_.name) == partitionColumns.map(_.name))

    StructType(partitionFileds)
  }

  def dataSchema: StructType = {
    val dataFields = schema.dropRight(partitionColumns.length)
    assert(dataFields.map(_.name) == dataColumns.map(_.name))
    StructType(dataFields)
  }
}

case class OdpsRelation(table: OdpsTable,
                        dataCols: Seq[AttributeReference],
                        partitionCols: Seq[AttributeReference]) extends LeafNode with MultiInstanceRelation {
  override def newInstance(): LogicalPlan = {
    copy(
      dataCols = dataCols.map(_.newInstance()),
      partitionCols = partitionCols.map(_.newInstance())
    )
  }

  override def output: Seq[Attribute] =
    dataCols ++ partitionCols

  override def equals(relation: Any): Boolean = relation match {
    case other: OdpsRelation => table.name == other.table.name && table.database == other.table.database && output == other.output
    case _ => false
  }

  override def hashCode(): Int = {
    Objects.hashCode((table.database + "." + table.name), output)
  }

  override def statistics: Statistics = {
    Statistics(sizeInBytes = BigInt(OdpsMetastoreCatalog.getSize(table.name)))
  }
}

class OdpsMetastoreCatalog(val conf: CatalystConf) extends Catalog with Logging {
  import OdpsMetastoreCatalog._

  private[this] val tables: ConcurrentMap[String, LogicalPlan] =
    new ConcurrentHashMap[String, LogicalPlan]

  def getDefaultProject(): String = {
    odps.getDefaultProject
  }

  def getQualifiedTableName(tableIdent: TableIdentifier): QualifiedTableName = {
    QualifiedTableName(
      tableIdent.database.getOrElse(odps.getDefaultProject).toLowerCase,
      tableIdent.table.toLowerCase)
  }

  override def tableExists(tableIdent: TableIdentifier): Boolean = {
    odps.tables().exists(tableIdent.database.getOrElse(odps.getDefaultProject), tableIdent.table)
  }

  def getTable(db: String, tableName: String): OdpsTable = {
    require(odps.tables().exists(tableName))
    val project = if (db.isEmpty) odps.getDefaultProject else db
    val table = odps.tables().get(project, tableName)
    val data = table.getSchema.getColumns.asScala.map(c =>
      StructField(c.getName, typeInfo2Type(c.getTypeInfo), true)).toList
    val partitions = table.getSchema.getPartitionColumns.asScala.map(c =>
      StructField(c.getName, typeInfo2Type(c.getTypeInfo), true)).toList

    val dataCols = table.getSchema.getColumns.asScala.map(x => OdpsColumn(x.getName, x.getTypeInfo.getTypeName, x.getComment))
    val partitionCols = table.getSchema.getPartitionColumns.asScala.map(x => OdpsColumn(x.getName, x.getTypeInfo.getTypeName, x.getComment))

    OdpsTable(
      Some(odps.getDefaultProject),
      table.getName,
      dataCols,
      partitionCols,
      schema = StructType(
        data ::: partitions
      )
    )
  }

  def typeToName(dataType: DataType): String = {
    dataType match {
      case FloatType => "FLOAT"
      case DoubleType => "DOUBLE"
      case BooleanType => "BOOLEAN"
      case DateType => "DATE"
      case TimestampType => "TIMESTAMP"
      case ByteType => "TINYINT"
      case ShortType => "SMALLINT"
      case IntegerType => "INT"
      case LongType => "BIGINT"
      case StringType => "STRING"
      case BinaryType => "BINARY"
      case d: DecimalType => d.simpleString
      case a: ArrayType => a.simpleString
      case m: MapType => m.simpleString
      case s: StructType => s.simpleString
      case _ => throw
        new UnsupportedOperationException("Spark data type:" + dataType + " not supported!")
    }
  }

  private def getSQLString(projectName: String, tableName: String, schema: TableSchema,
                           comment: String, ifNotExists: Boolean, lifeCycle: Long) = {
    if (projectName != null && tableName != null && schema != null) {
      val sb = new StringBuilder
      sb.append("CREATE TABLE ")
      if (ifNotExists) sb.append(" IF NOT EXISTS ")
      sb.append(projectName).append(".`").append(tableName).append("` (")
      val columns = schema.getColumns
      var pcolumns = 0
      while (pcolumns < columns.size) {
        {
          val i = columns.get(pcolumns).asInstanceOf[Column]
          sb.append("`").append(i.getName).append("` ").append(i.getTypeInfo.getTypeName)
          if (i.getComment != null) sb.append(" COMMENT \'").append(i.getComment).append("\'")
          if (pcolumns + 1 < columns.size) sb.append(',')
        }
        {
          pcolumns += 1;
          pcolumns
        }
      }
      sb.append(')')
      if (comment != null) sb.append(" COMMENT \'" + comment + "\' ")
      val var12 = schema.getPartitionColumns
      if (var12.size > 0) {
        sb.append(" PARTITIONED BY (")
        var var13 = 0
        while (var13 < var12.size) {
          {
            val c = var12.get(var13).asInstanceOf[Column]
            sb.append(c.getName).append(" ").append(c.getTypeInfo.getTypeName)
            if (c.getComment != null) sb.append(" COMMENT \'").append(c.getComment).append("\'")
            if (var13 + 1 < var12.size) sb.append(',')
          }
          {
            var13 += 1;
            var13
          }
        }
        sb.append(')')
      }
      if (lifeCycle != null && lifeCycle != 0) sb.append(" LIFECYCLE ").append(lifeCycle)
      sb.append(';')
      sb.toString
    } else throw new IllegalArgumentException
  }

  def createTable(tableDefinition: OdpsTable, ignoreIfExists: Boolean): Unit = {
    val tableSchema = new TableSchema
    tableDefinition.partitionSchema.foreach(
      f => tableSchema.addPartitionColumn(
        new Column(f.name, TypeInfoParser.getTypeInfoFromTypeString(
          typeToName(f.dataType).replaceAll("`", "")), null))
    )
    tableDefinition.schema.filter(
      f => !tableDefinition.partitionColumns.map(_.name).contains(f.name)).foreach(
      f => tableSchema.addColumn(
        new Column(f.name, TypeInfoParser.getTypeInfoFromTypeString(
          typeToName(f.dataType).replaceAll("`", "")), null)
      )
    )
    SQLTask.run(odps, getSQLString(
      tableDefinition.database,
      tableDefinition.name, tableSchema, null, ignoreIfExists,
      null.asInstanceOf[Long]))
      .waitForSuccess()
  }

  def dropTable(
                 db: String,
                 table: String,
                 ignoreIfNotExists: Boolean,
                 purge: Boolean): Unit = {
    doDropTable(db, table, ignoreIfNotExists, purge)
  }

  def doDropTable(
                   db: String,
                   table: String,
                   ignoreIfNotExists: Boolean,
                   purge: Boolean): Unit = {
    if (!odps.tables().exists(table)) {
      return
    }
    val tbl = odps.tables().get(db, table)
    if (tbl.isVirtualView) {
      SQLTask.run(odps, "DROP VIEW " + db + "." + table + ";").waitForSuccess()
    } else {
      val dropSql = new StringBuilder
      dropSql.append("DROP TABLE ")
      if (ignoreIfNotExists) {
        dropSql.append(" IF EXISTS ")
      }

      dropSql.append(db).append(".").append(table).append(";")
      SQLTask.run(odps, dropSql.toString()).waitForSuccess()
    }
  }

  def lookupRelation(
                      tableIdent: TableIdentifier,
                      alias: Option[String]): LogicalPlan = {
    val qualifiedTableName = getQualifiedTableName(tableIdent)
    if (odps.tables().exists(qualifiedTableName.database, qualifiedTableName.name)) {
      val table = getTable(qualifiedTableName.database, qualifiedTableName.name)

      val qualifiedTable = OdpsRelation(
        table,
        table.dataSchema.asNullable.toAttributes,
        table.partitionSchema.asNullable.toAttributes
      )

      // If an alias was specified by the lookup, wrap the plan in a subquery so that attributes are
      // properly qualified with this alias.
      alias.map(a => Subquery(a, qualifiedTable)).getOrElse(qualifiedTable)
    } else {
      val tableName = getTableName(tableIdent)
      val table = tables.get(tableName)
      if (table == null) {
        throw new NoSuchTableException
      }
      val tableWithQualifiers = Subquery(tableName, table)

      // If an alias was specified by the lookup, wrap the plan in a subquery so that attributes are
      // properly qualified with this alias.
      alias.map(a => Subquery(a, tableWithQualifiers)).getOrElse(tableWithQualifiers)
    }
  }

  /**
    * Returns tuples of (tableName, isTemporary) for all tables in the given database.
    * isTemporary is a Boolean value indicates if a table is a temporary or not.
    */
  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    odps.tables().asScala.map(x => {
      (x.getName, false)
    }).toList
  }

  def createPartitions(
                        db: String,
                        table: String,
                        parts: Seq[Map[String, String]],
                        ignoreIfExists: Boolean): Unit = {
    val partitionColumns = odps.tables().get(db, table).getSchema.getPartitionColumns
    parts.foreach(p => odps.tables().get(db, table).createPartition(new PartitionSpec(
      partitionColumns.asScala.map(
        c => c.getName + "=" + p.get(c.getName)
      ).mkString(",")
    ), ignoreIfExists))
  }

  override def refreshTable(tableIdent: TableIdentifier): Unit = {}

  override def registerTable(tableIdent: TableIdentifier, plan: LogicalPlan): Unit = {
    tables.put(getTableName(tableIdent), plan)
  }

  override def unregisterTable(tableIdent: TableIdentifier): Unit = {
    tables.remove(getTableName(tableIdent))
  }

  override def unregisterAllTables(): Unit = {
    tables.clear()
  }
}

object OdpsMetastoreCatalog {
  val odps: Odps = {
    val hints = collection.mutable.Map[String, String]()
    hints.put("odps.sql.preparse.odps2", "lot")
    hints.put("odps.sql.planner.parser.odps2", "true")
    hints.put("odps.sql.planner.mode", "lot")
    hints.put("odps.compiler.output.format", "lot,pot")
    hints.put("odps.compiler.playback", "true")
    hints.put("odps.compiler.warning.disable", "false")
    hints.put("odps.sql.ddl.odps2", "true")
    hints.put("odps.sql.runtime.mode", "EXECUTIONENGINE")
    hints.put("odps.sql.sqltask.new", "true")
    hints.put("odps.sql.hive.compatible", "true")
    hints.put("odps.compiler.verify", "true")
    hints.put("odps.sql.decimal.odps2", "true")
    SQLTask.setDefaultHints(hints.asJava)
    CupidSession.get.odps
  }

  /** Given the string representation of a type, return its DataType */
  def typeInfo2Type(typeInfo: TypeInfo): DataType = {
    val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
    val CHAR = """char\(\s*(\d+)\s*\)""".r
    val VARCHAR = """varchar\(\s*(\d+)\s*\)""".r
    val ARRAY = """array<\s*(.+)\s*>""".r
    val MAP = """(map<\s*.+\s*>)""".r
    val STRUCT = """(struct<\s*.+\s*>)""".r

    typeInfo.getTypeName.toLowerCase match {
      case "decimal" => DecimalType(typeInfo.asInstanceOf[DecimalTypeInfo].getPrecision,
        typeInfo.asInstanceOf[DecimalTypeInfo].getScale)
      case FIXED_DECIMAL(precision, scale) => DecimalType(precision.toInt, scale.toInt)
      case "float" => FloatType
      case "double" => DoubleType
      case "boolean" => BooleanType
      case "datetime" => TimestampType
      case "date" => DateType
      case "timestamp" => TimestampType
      case "tinyint" => ByteType
      case "smallint" => ShortType
      case "int" => IntegerType
      case "bigint" => LongType
      case "string" => StringType
      case CHAR(_) => StringType
      case VARCHAR(_) => StringType
      case "binary" => BinaryType
      case ARRAY(_) => ArrayType(typeInfo2Type(typeInfo.asInstanceOf[ArrayTypeInfo]
        .getElementTypeInfo))
      case MAP(_) => genMapType(typeInfo)
      case STRUCT(_) => genStructType(typeInfo)
      case _ => throw
        new UnsupportedOperationException("ODPS data type: " + typeInfo + " not supported!")
    }
  }

  private def genMapType(typeInfo: TypeInfo): MapType = {
    val mti = typeInfo.asInstanceOf[MapTypeInfo]
    MapType(typeInfo2Type(mti.getKeyTypeInfo), typeInfo2Type(mti.getValueTypeInfo))
  }

  private def genStructType(typeInfo: TypeInfo): StructType = {
    val sti = typeInfo.asInstanceOf[StructTypeInfo]
    StructType(sti.getFieldNames.toArray().zip(sti.getFieldTypeInfos.toArray()).map(pair => {
      StructField(pair._1.asInstanceOf[String],
        typeInfo2Type(pair._2.asInstanceOf[TypeInfo]))
    }))
  }

  def getPartitions(relation: OdpsRelation, partitionPruningPred: Seq[Expression]): Array[String] = {
    val partitionSchema = relation.table.partitionSchema
    val boundPredicate = if (partitionPruningPred.nonEmpty) {
      Some(InterpretedPredicate.create(partitionPruningPred.reduce(And).transform {
        case attr: AttributeReference => {
          val index = partitionSchema.indexWhere(_.name == attr.name)
          BoundReference(index, partitionSchema(index).dataType, nullable = true)
        }
      }))
    } else None
    val partitions = odps.tables().get(relation.table.name).getPartitions.asScala.map(p => {
      val partitionMap = new mutable.LinkedHashMap[String, String]
      p.getPartitionSpec.keys().asScala.foreach(key => {
        partitionMap.put(key, p.getPartitionSpec.get(key))
      })
      val row = InternalRow.fromSeq(partitionSchema.map { field =>
        val partValue = partitionMap.get(field.name).get
        Cast(Literal(partValue), field.dataType).eval()
      })
      if (!boundPredicate.isDefined) Some(partitionMap)
      else if (boundPredicate.get.apply(row)) Some(partitionMap)
      else None
    })
    partitions.flatMap(x => {
      x.map(x => x.keys.map(k => s"$k=${x.get(k).get}").toArray.mkString(","))
    }).toArray
  }

  def getSize(table: String): Long = {
    odps.tables().get(table).getPhysicalSize
  }
}