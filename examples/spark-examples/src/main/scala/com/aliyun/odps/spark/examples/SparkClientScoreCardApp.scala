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

import java.nio.ByteBuffer
import java.util.HashMap

import com.aliyun.odps.cupid.client.spark.api.JobStatus._
import com.aliyun.odps.cupid.client.spark.client.CupidSparkClientRunner
import com.aliyun.odps.cupid.client.spark.util.serializer.JavaSerializerInstance

import scala.actors.threadpool.{ExecutorService, Executors, Future}
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class SparkClientScoreCardApp(cupidSparkClient: CupidSparkClientRunner) {

  def runScoreCard(jarPath: String, partitionNum: Int, concurrentNum: Int): Unit = {
    val jarName = cupidSparkClient.addJar(jarPath)
    //1.cache the parallrdd
    val cacheSourceRddName = runCacheJob(jarName, partitionNum)
    //2.run the concurrent join
    val pool: ExecutorService = Executors.newFixedThreadPool(concurrentNum)
    val futures = new ArrayBuffer[Future]()
    for (i <- 0 to concurrentNum) {
      futures.append(pool.submit(new Join(cupidSparkClient, jarName, cacheSourceRddName, partitionNum)))
    }
    pool.shutdown()
    for (f <- futures) {
      try {
        f.get()
      } catch {
        case ex: Exception => println(ex.getMessage())
      }
    }
    pool.shutdownNow()
  }

  private def runCacheJob(jarName: String, partitionNum: Int): String = {
    val configArgs = new HashMap[String, String]()
    val cacheSourceRddName = "ScoreCardSourceCacheRdd"
    configArgs.put("CacheSourceRddName", cacheSourceRddName)
    configArgs.put("partitionNum", partitionNum.toString)
    val cacheRddAndCountJobId = cupidSparkClient.startJob("com.aliyun.odps.cupid.client.spark.stable.CacheSourceRddJob", jarName, configArgs)
    try {
      val useCacheRddResult = getJobResult[java.lang.String](cacheRddAndCountJobId)
      println("Jobid = " + cacheRddAndCountJobId + "The  resutl is ")
      println(useCacheRddResult)
      useCacheRddResult
    } catch {
      case e: Exception => {
        println("Jobid = " + cacheRddAndCountJobId + ",printStackTrace is:")
        e.printStackTrace()
        throw e
      }
    }
  }

  private def getJobResult[T: ClassTag](jobId: String): T = {
    var resultBytes: Array[Byte] = null
    cupidSparkClient.getJobResult(jobId) match {
      case JobSuccess(jobId, msg) => {
        resultBytes = msg
        JavaSerializerInstance.getInstance.deserialize[T](ByteBuffer.wrap(resultBytes))
      }
      case JobFailed(jobId, msg) => throw new Exception("jobid = " + jobId + ",jobStatus = jobfailed," + ",msg=" + msg)
      case JobKilled(jobId) => throw new Exception("jobid = " + jobId + ",jobStatus = jobkilled")
    }
  }

  private class Join(cupidSparkClient: CupidSparkClientRunner, jarName: String, cacheSourceRddName: String, partitionNum: Int) extends Runnable {
    def run() {
      val configArgs = new HashMap[String, String]()
      configArgs.put("CacheSourceRddName", cacheSourceRddName)
      configArgs.put("partitionNum", partitionNum.toString)
      val scoreCardJoinJobId = cupidSparkClient.startJob("com.aliyun.odps.cupid.client.spark.stable.ScoreCardJoin", jarName, configArgs)
      try {
        val useCacheRddResult = getJobResult[Long](scoreCardJoinJobId)
        println("Jobid = " + scoreCardJoinJobId + ",The  resutl is ")
        println(useCacheRddResult)
        assert(useCacheRddResult == 10000)
      } catch {
        case e: Exception => {
          println("Jobid = " + scoreCardJoinJobId + ",printStackTrace is:")
          e.printStackTrace()
          throw e
        }
      }
    }
  }
}
