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

package com.aliyun.odps.cupid.client.spark.stable

import java.nio.ByteBuffer
import java.util.HashMap

import com.aliyun.odps.cupid.client.spark.api.{JobContext, SparkJob}
import com.aliyun.odps.cupid.client.spark.service.JobContextImpl
import com.aliyun.odps.cupid.client.spark.util.serializer.JavaSerializerInstance
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object ScoreCardJoin extends SparkJob {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("WordCountExample")
    val sc = new SparkContext(conf)
    val jobContext = new JobContextImpl(sc, null)
    // mock put the cache rdd
    val re = jobContext.sc().parallelize(1 to 10000, 10).map(one => (one, 1)).partitionBy(new HashPartitioner(10)).cache()
    println("re count:" + re.count())
    val CacheSourceRddName = "ScoreCardSourceCacheRdd"
    jobContext.putNamedObject(CacheSourceRddName, re)

    //mock the config
    val configArgs = new HashMap[String, String]()
    configArgs.put("CacheSourceRddName", CacheSourceRddName)
    val config = ConfigFactory.parseMap(configArgs)

    val resultBytes = runJob(jobContext, config)
    val result = JavaSerializerInstance.getInstance.deserialize[Long](ByteBuffer.wrap(resultBytes))
    println("Result is:" + result)
  }

  override def runJob(jobContext: JobContext, jobConf: Config): Array[Byte] = {
    if (!jobConf.hasPath("CacheSourceRddName")) {
      throw new Exception("The jobConf do not has CacheSourceRddName")
    }
    val cacheRddName = jobConf.getString("CacheSourceRddName")
    val cacheRdd = getTheCacheRDD(jobContext, cacheRddName)
    if (cacheRdd == None) {
      throw new Exception("Do not get the cacheRDD :" + cacheRddName)
    }
    var partitonNum = 10
    if (jobConf.hasPath("partitonNum")) partitonNum = jobConf.getInt("partitonNum")
    val q = jobContext.sc().parallelize(1 to 10000, partitonNum).map(one => (one, 1)).partitionBy(new HashPartitioner(partitonNum))
    val result = (cacheRdd.get).join(q).count()
    JavaSerializerInstance.getInstance.serialize(result).array()
  }

  private def getTheCacheRDD(jobContext: JobContext, cacheRDDName: String): Option[RDD[(Int, Int)]] = {
    val cacheRdd = jobContext.getNamedObject(cacheRDDName) match {
      case rdd: RDD[(Int, Int)] => {
        Some(rdd)
      }
      case _ => {
        None
      }
    }
    cacheRdd
  }
}


object CacheSourceRddJob extends SparkJob {
  override def runJob(jobContext: JobContext, jobConf: Config): Array[Byte] = {
    if (!jobConf.hasPath("CacheSourceRddName")) {
      throw new Exception("The jobConf do not has CacheSourceRddName")
    }
    val cacheRddName = jobConf.getString("CacheSourceRddName")
    var partitionNum = 10
    if (jobConf.hasPath("partitionNum")) partitionNum = jobConf.getInt("partitionNum")
    val re = jobContext.sc().parallelize(1 to 10000, partitionNum).map(one => (one, 1)).partitionBy(new HashPartitioner(partitionNum)).cache()
    println("re count:" + re.count())
    jobContext.putNamedObject(cacheRddName, re)
    JavaSerializerInstance.getInstance.serialize(cacheRddName).array()
  }
}


