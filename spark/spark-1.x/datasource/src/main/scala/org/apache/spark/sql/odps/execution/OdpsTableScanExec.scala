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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeMap, AttributeReference, AttributeSet, BoundReference, Expression, InterpretedPredicate, UnsafeProjection}
import org.apache.spark.sql.execution.LeafNode
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.odps.{OdpsInternalRDD, OdpsMetastoreCatalog, OdpsRelation}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

case class OdpsTableScanExec(
                              requestedAttributes: Seq[Attribute],
                              relation: OdpsRelation,
                              partitionPruningPred: Seq[Expression])(
                              @transient val context: SQLContext)
  extends LeafNode {

  @transient
  val hadoopConf = sparkContext.hadoopConfiguration

  override lazy val metrics = Map(
    "numInputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of input rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))


  def producedAttributes: AttributeSet = outputSet ++
    AttributeSet(partitionPruningPred.flatMap(_.references))

  private val originalAttributes = AttributeMap(relation.output.map(a => a -> a))

  override val output: Seq[Attribute] = {
    // Retrieve the original attributes based on expression ID so that capitalization matches.
    requestedAttributes.map(originalAttributes)
  }

  /**
    * Overridden by concrete implementations of SparkPlan.
    * Produces the result of the query as an RDD[InternalRow]
    */
  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    // Avoid to serialize MetastoreRelation because schema is lazy. (see SPARK-15649)
    val outputSchema = schema

    // prepare partition for OdpsInternalRDD in format Array[String]("pt1=1,pt2=2", "pt1=1,pt2=3")
    val partStrArray: Array[String] = relation.table.partitionColumns.length match {
      case 0 => Array[String]()
      case _ => OdpsMetastoreCatalog.getPartitions(relation, partitionPruningPred)
    }

    val concurrentNum = Math.max(1, Math.min(partStrArray.length / 50, 40))

    val partSplits = collection.mutable.Map[Int, ArrayBuffer[String]]()

    partStrArray.zipWithIndex.foreach {
      case (x, i) =>
        val key = if (concurrentNum == 1) 1 else i % concurrentNum
        partSplits.getOrElse(key, {
          val pList = ArrayBuffer[String]()
          partSplits.put(key, pList)
          pList
        }) += x
    }

    val rdd = if (partSplits.isEmpty) {
      new OdpsInternalRDD(
        sparkContext,
        hadoopConf.get("odps.access.id", null),
        hadoopConf.get("odps.access.key", null),
        hadoopConf.get("odps.end.point"),
        relation.table.database,
        relation.table.name,
        output.map(_.name).toArray,
        partStrArray,
        sparkContext.isLocal,
        0, // TODO add another conf for further control
        splitSize = sparkContext.conf.getInt("odps.input.split.size", 256),
        relation.table.partitionColumns.length
      )
    } else {
      val future = Future.sequence(partSplits.keys.map(key =>
        Future[RDD[org.apache.spark.sql.catalyst.InternalRow]] {
          val rdd = new OdpsInternalRDD(
            sparkContext,
            hadoopConf.get("odps.access.id", null),
            hadoopConf.get("odps.access.key", null),
            hadoopConf.get("odps.end.point"),
            relation.table.database,
            relation.table.name,
            output.map(_.name).toArray,
            partSplits.get(key).get.toArray,
            sparkContext.isLocal,
            0, // TODO add another conf for further control
            splitSize = sparkContext.conf.getInt("odps.input.split.size", 256),
            relation.table.partitionColumns.length
          )
          rdd.partitions
          rdd
        }))
      val futureResults = Await.result(future, Duration(1, HOURS))
      if (futureResults.size == 1) {
        futureResults.head
      } else {
        sparkContext.union(futureResults.toSeq)
      }
    }
    rdd.mapPartitionsWithIndex { (index, iter) =>
      val proj = UnsafeProjection.create(outputSchema)
      iter.map { r =>
        numOutputRows += 1
        proj(r)
      }
    }
  }
}
