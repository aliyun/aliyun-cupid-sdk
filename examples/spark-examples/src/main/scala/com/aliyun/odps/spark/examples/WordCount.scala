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

package com.aliyun.odps.spark.examples

import org.apache.spark.sql.SparkSession

/**
  * Computes an approximation to pi
  * 1. build aliyun-cupid-sdk
  * 2. properly set spark.defaults.conf
  * 3. bin/spark-submit --master yarn-cluster --class com.aliyun.odps.spark.examples.WordCount
  * /path/to/aliyun-cupid-sdk/examples/spark-examples/target/spark-examples_2.11-1.0.0-SNAPSHOT-shaded.jar
  */
object WordCount {
  /**
    * To run this demo in IDEA, you need to do following:
    *   1. add spark-assembly jar into Project dependency
    *   2. use the local mode statement
    */
  def main(args: Array[String]) {
    // local mode statement
    // val spark = SparkSession
    //   .builder()
    //   .appName("WordCount")
    //   .master("local[4]")
    //   .getOrCreate()

    val spark = SparkSession
      .builder()
      .appName("WordCount")
      .getOrCreate()
    val sc = spark.sparkContext
    try {
      sc.parallelize(1 to 100, 10).map(word => (word, 1)).reduceByKey(_ + _, 10).take(100).foreach(println)
    } finally {
      sc.stop()
    }
  }
}