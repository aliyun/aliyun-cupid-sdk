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

import com.aliyun.odps.cupid.client.spark.client.CupidSparkClientRunner

/**
  * 1. build aliyun-cupid-sdk
  * 2. properly set spark.defaults.conf
  * 3. set $SPARK_HOME
  * 4. java -cp /path/to/aliyun-cupid-sdk/examples/spark-examples/target/spark-examples_2.11-1.0.0-SNAPSHOT-shaded.jar:$SPARK_HOME/jars/\* \
  *  com.aliyun.odps.spark.examples.SparkClientScoreCardFT partionNum concurrentNum \
  * /path/to/aliyun-cupid-sdk/examples/client-jobexamples/target/client-jobexamples_2.11-1.0.0-SNAPSHOT.jar
  */
object SparkClientScoreCardFT {
  def main(args: Array[String]) {
    var partitionNum = 10
    var concurrentNum = 20
    if (args.length > 1) {
      partitionNum = args(0).toInt
      concurrentNum = args(1).toInt
    }
    val cupidSparkClient = CupidSparkClientRunner.getReadyCupidSparkClient()
    val jarPath = args(2) //client-jobexamples jar path
    val sparkClientScoreCardApp = new SparkClientScoreCardApp(cupidSparkClient)
    sparkClientScoreCardApp.runScoreCard(jarPath, partitionNum, concurrentNum)
    cupidSparkClient.stopRemoteDriver()
  }
}
