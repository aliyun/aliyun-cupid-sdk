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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeSet, Cast, Expression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{CreateTableUsingAsSelect, CreateTempTableUsing, DescribeCommand => LogicalDescribeCommand}
import org.apache.spark.sql.execution.{DescribeCommand => RunnableDescribeCommand, _}
import org.apache.spark.sql.odps.execution._
import org.apache.spark.sql.{SQLContext, Strategy}

object OdpsAnalysis extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case InsertIntoTable(r: OdpsRelation, partSpec, query, overwrite, ifPartitionNotExists) =>
      InsertIntoOdpsTable(r, partSpec, query, overwrite, ifPartitionNotExists)
  }
}

object PreOdpsInsertCastAndRename extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // Wait until children are resolved.
    case p: LogicalPlan if !p.childrenResolved => p

    // We are inserting into an OdpsRelation
    case i @ InsertIntoOdpsTable(l , _, child, _, _) => {
      // First, make sure the data to be inserted have the same number of fields with the
      // schema of the relation.
      if (l.dataCols.size != child.output.size) {
        sys.error(
          s"$l requires that the query in the SELECT clause of the INSERT INTO/OVERWRITE " +
            s"statement generates the same number of columns as its schema.")
      }
      castAndRenameChildOutput(i, l.output, child)
    }
  }

  /** If necessary, cast data types and rename fields to the expected types and names. */
  def castAndRenameChildOutput(
                                insertIntoOdps: InsertIntoOdpsTable,
                                expectedOutput: Seq[Attribute],
                                child: LogicalPlan): InsertIntoOdpsTable = {
    val newChildOutput = expectedOutput.zip(child.output).map {
      case (expected, actual) =>
        val needCast = !expected.dataType.sameType(actual.dataType)
        // We want to make sure the filed names in the data to be inserted exactly match
        // names in the schema.
        val needRename = expected.name != actual.name
        (needCast, needRename) match {
          case (true, _) => Alias(Cast(actual, expected.dataType), expected.name)()
          case (false, true) => Alias(actual, expected.name)()
          case (_, _) => actual
        }
    }

    if (newChildOutput == child.output) {
      insertIntoOdps
    } else {
      insertIntoOdps.copy(query = Project(newChildOutput, child))
    }
  }
}

trait OdpsStrategies {
  self: SparkPlanner =>

  val sqlContext: SQLContext

  object OdpsTableScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, predicates, relation: OdpsRelation) => {
        val partitionKeyIds = AttributeSet(relation.partitionCols)
        val (pruningPredicates, otherPredicates) = predicates.partition { predicate =>
          !predicate.references.isEmpty &&
            predicate.references.subsetOf(partitionKeyIds)
        }

        pruneFilterProject(
          projectList,
          otherPredicates,
          identity[Seq[Expression]],
          OdpsTableScanExec(_, relation, pruningPredicates)(sqlContext)) :: Nil
      }
      case _ => Nil
    }
  }

  object OdpsDDLStrategy extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case CreateTableUsing(tableIdent, userSpecifiedSchema, provider, true, opts, false, _, _) =>
        ExecutedCommand(CreateTempTableUsing(tableIdent, userSpecifiedSchema, provider, opts)) :: Nil

      case c: CreateTableUsing if !c.temporary => {
        ExecutedCommand(CreateOdpsTableCommand(c.tableIdent, c.userSpecifiedSchema.get, c.partitionedSchema, true)) :: Nil
      }

      case c: CreateTableUsing if c.temporary && c.allowExisting =>
        sys.error("allowExisting should be set to false when creating a temporary table.")

      case CreateTableUsingAsSelect(tableIdent, provider, _, _, mode, opts, query) =>
        ExecutedCommand(CreateOdpsTableAsSelectCommand(tableIdent, query, true)) :: Nil

      case DropTable(tableIdent, ignoreIfNotExists) =>
        ExecutedCommand(DropTableCommand(tableIdent, ignoreIfNotExists)) :: Nil

      case describe @ LogicalDescribeCommand(table, isExtended) =>
        val resultPlan = self.sqlContext.executePlan(table).executedPlan
        ExecutedCommand(
          RunnableDescribeCommand(resultPlan, describe.output, isExtended)) :: Nil

      case logical.ShowFunctions(db, pattern) => ExecutedCommand(ShowFunctions(db, pattern)) :: Nil

      case logical.DescribeFunction(function, extended) =>
        ExecutedCommand(DescribeFunction(function, extended)) :: Nil

      case _ => Nil
    }
  }
}

