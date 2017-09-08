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

package com.aliyun.odps.cupid.client.spark

import java.nio.ByteBuffer
import java.util.HashMap

import com.aliyun.odps.cupid.client.spark.api.{JobContext, SparkJob}
import com.aliyun.odps.cupid.client.spark.service.JobContextImpl
import com.aliyun.odps.cupid.client.spark.util.serializer.JavaSerializerInstance
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}

object ParallRddJob extends SparkJob {
  // for local debug
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("WordCountExample")
    val sc = new SparkContext(conf)
    val jobContext = new JobContextImpl(sc, null)
    val configArgs = new HashMap[String, String]()
    configArgs.put("partitionNum", "2")
    val config = ConfigFactory.parseMap(configArgs)
    val resultBytes = runJob(jobContext, config)
    val result = JavaSerializerInstance.getInstance.deserialize[Long](ByteBuffer.wrap(resultBytes))
    println("Result is:" + result)
  }

  override def runJob(jobContext: JobContext, jobConf: Config): Array[Byte] = {
    val partitionNum = jobConf.getInt("partitionNum")
    println("partitionNum is =" + partitionNum)
    val sourceData = jobContext.sc().parallelize(0 to 10, partitionNum)
    val result = sourceData.count()
    JavaSerializerInstance.getInstance.serialize(result).array()
  }
}
