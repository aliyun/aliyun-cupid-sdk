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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.odps.{OdpsColumn, OdpsMetastoreCatalog, OdpsTable}
import org.apache.spark.sql.types.StructType

case class CreateOdpsTableCommand(tabelIdent: TableIdentifier,
                                  dataColsSchema: StructType,
                                  partitionColsSchema: Option[StructType],
                                  ignoreIfExists: Boolean)
extends RunnableCommand
{
  override def run(sqlContext: SQLContext): Seq[Row] = {
    val catalog = sqlContext.catalog.asInstanceOf[OdpsMetastoreCatalog]

    val table = OdpsTable(
      Some(tabelIdent.database.getOrElse(catalog.getDefaultProject())),
      tabelIdent.table,
      dataColsSchema.map(x => OdpsColumn(x.name, catalog.typeToName(x.dataType), "")),
      if (partitionColsSchema.isDefined)
        partitionColsSchema.get.map(x => OdpsColumn(x.name, catalog.typeToName(x.dataType), ""))
      else
        Nil,
      if (partitionColsSchema.isDefined)
      StructType(dataColsSchema.map(x => x) ++ partitionColsSchema.get.map(x => x))
      else dataColsSchema
    )

    catalog.createTable(table, ignoreIfExists)

    Seq.empty[Row]
  }
}
