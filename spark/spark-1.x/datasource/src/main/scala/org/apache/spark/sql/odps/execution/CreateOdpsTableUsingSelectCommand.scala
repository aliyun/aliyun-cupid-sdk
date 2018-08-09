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

import com.aliyun.odps.`type`.TypeInfoParser
import org.apache.spark.sql.catalyst.TableIdentifier

import scala.util.control.NonFatal
import org.apache.spark.sql.{AnalysisException, Row, SQLContext}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan}
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.odps.{OdpsColumn, OdpsMetastoreCatalog, OdpsTable}


/**
  * Create table and insert the query result into it.
  *
  * @param tableIdent     the Table Describe.
  * @param query          the query whose result will be insert into the new relation
  * @param ignoreIfExists allow continue working if it's already exists, otherwise
  *                       raise exception
  */
case class CreateOdpsTableAsSelectCommand(
                                           tableIdent: TableIdentifier,
                                           query: LogicalPlan,
                                           ignoreIfExists: Boolean)
  extends RunnableCommand {

  def innerChildren: Seq[LogicalPlan] = Seq(query)

  override def run(sqlContext: SQLContext): Seq[Row] = {
    if (sqlContext.catalog.tableExists(tableIdent)) {
      if (ignoreIfExists) {
        // Do Nothing
      } else {
        throw new AnalysisException(s"$tableIdent already exists.")
      }
    } else {
      // TODO ideally, we should get the output data ready first and then
      // add the relation into catalog, just in case of failure occurs while data
      // processing.
      val catalog = sqlContext.catalog.asInstanceOf[OdpsMetastoreCatalog]

      val table = OdpsTable(
        Some(tableIdent.database.getOrElse(catalog.getDefaultProject())),
        tableIdent.table,
        query.schema.map(x => OdpsColumn(x.name,
          TypeInfoParser.getTypeInfoFromTypeString(catalog.typeToName(x.dataType).replaceAll("`", "")).getTypeName, "")),
        Nil,
        query.schema
      )

      catalog.createTable(
        table.copy(schema = query.schema), ignoreIfExists = false)

      try {
        sqlContext.executePlan(
          InsertIntoTable(
            UnresolvedRelation(tableIdent),
            Map(),
            query,
            overwrite = true,
            ifNotExists = false)).toRdd
      } catch {
        case NonFatal(e) =>
          // drop the created table.
          catalog.dropTable(tableIdent.database.get, tableIdent.table,
            ignoreIfNotExists = true,
            purge = false)
          throw e
      }
    }

    Seq.empty[Row]
  }

  override def argString: String = {
    s"[Database:${tableIdent.database.get}}, " +
      s"TableName: ${tableIdent.table}, " +
      s"InsertIntoOdpsTable]"
  }
}
