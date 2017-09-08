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

package com.aliyun.odps.spark.examples.multiclient

import java.io._

import com.aliyun.odps.cupid.client.spark.client.CupidSparkClientRunner
import com.aliyun.odps.spark.examples.SparkClientNormalApp

object ClientBindToServer {

  def main(args: Array[String]) {
    val lookupName = readLookupNameToFile
    val jarPath = args(0) //client-jobexamples jar path
    val cupidSparkClient = CupidSparkClientRunner.getSparkClientWithLookupName(lookupName)
    val sparkClientNormalApp = new SparkClientNormalApp(cupidSparkClient)
    sparkClientNormalApp.runNormalJob(jarPath)
    cupidSparkClient.stopRemoteDriver()
  }

  def readLookupNameToFile(): String = {
    val pathName = "/tmp/SparkOnOdpsLookupName"
    val fileName = new File(pathName)

    val reader = new InputStreamReader(new FileInputStream(fileName))
    val br = new BufferedReader(reader)
    val lookUpName = br.readLine()
    br.close()
    lookUpName
  }
}