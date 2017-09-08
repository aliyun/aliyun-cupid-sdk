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
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UseCacheRdd extends SparkJob {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("WordCountExample")
    val sc = new SparkContext(conf)
    val jobContext = new JobContextImpl(sc, null)
    // mock put the cache rdd
    // val cacheParallRdd = jobContext.sc().parallelize(Seq[Int](1, 2, 3), 1)
    val cacheParallRdd = jobContext.sc().parallelize(0 to 10, 1)
    val cacheRDDName = "parallRdd"
    jobContext.putNamedObject(cacheRDDName, cacheParallRdd)

    //mock the config
    val configArgs = new HashMap[String, String]()
    configArgs.put("cacheRDDName", "parallRdd")
    val config = ConfigFactory.parseMap(configArgs)

    val resultBytes = runJob(jobContext, config)
    val result = JavaSerializerInstance.getInstance.deserialize[Array[Int]](ByteBuffer.wrap(resultBytes))
    println("Result is:")
    result.foreach(println)
  }

  override def runJob(jobContext: JobContext, jobConf: Config): Array[Byte] = {
    val cacheRDDName = jobConf.getString("cacheRDDName")
    println("cacheRddName=" + cacheRDDName)
    val nowParallRdd = jobContext.getNamedObject(cacheRDDName) match {
      case rdd: RDD[Int] => {
        println("TestCacheRdd get the rdd")
        Some(rdd)
      }
      case _ => {
        println("TestCacheRdd not get the rdd")
        None
      }
    }
    val collectResult = nowParallRdd.get.map(i => i + 1).collect()
    println("result is :")
    collectResult.foreach(println)
    JavaSerializerInstance.getInstance.serialize(collectResult).array()
  }
}
