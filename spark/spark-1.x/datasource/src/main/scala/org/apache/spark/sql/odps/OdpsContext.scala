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

import java.util.Properties

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.catalyst.analysis.{Analyzer, Catalog, CleanupAliases, ComputeCurrentTime, HiveTypeCoercion, ResolveUpCast}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, PreInsertCastAndRename, ResolveDataSource}
import org.apache.spark.sql.execution.ui.SQLListener
import org.apache.spark.sql.execution.{CacheManager, ExtractPythonUDFs, datasources}
import org.apache.spark.sql.{SQLContext, Strategy}
import org.apache.spark.{Logging, SparkContext}
import scala.collection.JavaConversions._

class OdpsContext(
                   sc: SparkContext,
                   cacheManager: CacheManager,
                   listener: SQLListener,
                   isRootContext: Boolean)
  extends SQLContext(sc, cacheManager, listener, isRootContext) with Logging {
  self =>

  override lazy val catalog: Catalog = new OdpsMetastoreCatalog(conf)

  val odpsPlanner = new SparkPlanner with OdpsStrategies {
    override def strategies: Seq[Strategy] = sqlContext.experimental.extraStrategies ++ (
      DataSourceStrategy ::
        OdpsDDLStrategy ::
        TakeOrderedAndProject ::
        Aggregation ::
        LeftSemiJoin ::
        EquiJoinSelection ::
        InMemoryScans ::
        OdpsTableScans ::
        BasicOperators ::
        BroadcastNestedLoop ::
        CartesianProduct ::
        DefaultJoin :: Nil)
  }

  val odpsAnalyzer: Analyzer =
    new Analyzer(catalog, functionRegistry, conf) {
      override val extendedResolutionRules =
        ExtractPythonUDFs ::
          PreInsertCastAndRename ::
          (if (conf.runSQLOnFile) new ResolveDataSource(self) :: Nil else Nil)

      override val extendedCheckRules = Seq(
        datasources.PreWriteCheck(catalog)
      )

      override lazy val batches: Seq[Batch] = Seq(
        Batch("Substitution", fixedPoint,
          CTESubstitution,
          WindowsSubstitution),
        Batch("Resolution", fixedPoint,
          ResolveRelations ::
            ResolveReferences ::
            ResolveGroupingAnalytics ::
            ResolvePivot ::
            ResolveUpCast ::
            ResolveSortReferences ::
            ResolveGenerate ::
            ResolveFunctions ::
            ResolveAliases ::
            ExtractWindowExpressions ::
            GlobalAggregates ::
            ResolveAggregateFunctions ::
            HiveTypeCoercion.typeCoercionRules ++
              extendedResolutionRules : _*),
        Batch("OdpsAnalysis", Once,
          OdpsAnalysis.asInstanceOf[Rule[LogicalPlan]],
          PreOdpsInsertCastAndRename
        ),
        Batch("Nondeterministic", Once,
          PullOutNondeterministic,
          ComputeCurrentTime),
        Batch("UDF", Once,
          HandleNullInputsForUDF),
        Batch("Cleanup", fixedPoint,
          CleanupAliases)
      )
    }

  def this(sparkContext: SparkContext) = {
    this(sparkContext, new CacheManager, SQLContext.createListenerAndUI(sparkContext), true)
  }

  def this(sparkContext: JavaSparkContext) = this(sparkContext.sc)

  @transient
  override protected[sql] val planner = odpsPlanner

  @transient
  override protected[sql] lazy val analyzer = odpsAnalyzer

  override def setConf(props: Properties): Unit = {
    props.foreach { case (k, v) => sc.hadoopConfiguration.set(k, v) }
  }

  override def setConf(key: String, value: String): Unit = {
    sc.hadoopConfiguration.set(key, value)
  }
}
