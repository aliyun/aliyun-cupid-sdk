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

package com.aliyun.odps.spark.examples.mllib

import org.apache.spark.mllib.clustering.KMeans._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * KmeansModelSaveToPanguForTemp OSS
  * 1. build aliyun-cupid-sdk
  * 2. properly set spark.defaults.conf
  * 3. bin/spark-submit --master yarn-cluster --class com.aliyun.odps.spark.examples.SparkPi
  * /path/to/aliyun-cupid-sdk/examples/spark-examples/target/spark-examples_2.11-1.0.0-SNAPSHOT-shaded.jar
  */
object KmeansModelSaveToPanguForTemp {
  // the fs.defaultFS is FUXI_JOB_TEMP_ROOT
  val modelOssDir = "mllib"

  def main(args: Array[String]) {
    //1. train and save the model
    val spark = SparkSession
      .builder()
      .appName("KmeansModelSaveToPanguForTemp")
      .getOrCreate()
    val sc = spark.sparkContext

    try {
      val points = Seq(
        Vectors.dense(0.0, 0.0),
        Vectors.dense(0.0, 0.1),
        Vectors.dense(0.1, 0.0),
        Vectors.dense(9.0, 0.0),
        Vectors.dense(9.0, 0.2),
        Vectors.dense(9.2, 0.0)
      )
      val rdd = sc.parallelize(points, 3)
      val initMode = K_MEANS_PARALLEL
      val model = KMeans.train(rdd, k = 2, maxIterations = 2, runs = 1, initMode)
      val predictResult1 = rdd.map(feature => "cluster id: " + model.predict(feature) + " feature:" + feature.toArray.mkString(",")).collect
      println("modelOssDir=" + modelOssDir)
      model.save(sc, modelOssDir)

      //2. predict from the oss model
      val modelLoadOss = KMeansModel.load(sc, modelOssDir)
      val predictResult2 = rdd.map(feature => "cluster id: " + modelLoadOss.predict(feature) + " feature:" + feature.toArray.mkString(",")).collect
      predictResult2.foreach(println)
      println("Summary:")
      assert(predictResult1.size == predictResult2.size)
      predictResult2.foreach(result2 => assert(predictResult1.contains(result2)))
      println("Job successful\n")
    } catch {
      case ex: Exception => {
        println("job failed!!!")
        throw ex
      }
    } finally {
      sc.stop
    }
  }
}
