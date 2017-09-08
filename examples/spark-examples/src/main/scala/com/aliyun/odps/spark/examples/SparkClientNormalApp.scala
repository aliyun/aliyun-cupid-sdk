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
import java.util
import java.util.HashMap

import com.aliyun.odps.cupid.client.spark.api.JobStatus._
import com.aliyun.odps.cupid.client.spark.client.CupidSparkClientRunner
import com.aliyun.odps.cupid.client.spark.util.serializer.JavaSerializerInstance

import scala.actors.threadpool.{ExecutorService, Executors, Future}
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class SparkClientNormalApp(cupidSparkClient: CupidSparkClientRunner) {

  def runNormalJob(jarPath: String): Unit = {
    val jarName = cupidSparkClient.addJar(jarPath)
    val pool: ExecutorService = Executors.newFixedThreadPool(20)
    val futures = new ArrayBuffer[Future]()
    futures.append(pool.submit(new UseCacheJob(cupidSparkClient, jarName)))
    futures.append(pool.submit(new SimpleJob(cupidSparkClient, jarName)))
    futures.append(pool.submit(new KillJob(cupidSparkClient, jarName)))
    pool.shutdown()
    for (f <- futures) {
      try {
        f.get()
      } catch {
        case ex: Exception => {
          println(ex.getMessage())
          throw ex
        }
      }
    }
    pool.shutdownNow()
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

  class UseCacheJob(cupidSparkClient: CupidSparkClientRunner, jarName: String) extends Runnable {
    def run() {
      val configArgs = new HashMap[String, String]()
      configArgs.put("cacheRDDName", "parallRdd")
      val cacheParallRddJobId = cupidSparkClient.startJob("com.aliyun.odps.cupid.client.spark.CacheRddJob", jarName, configArgs)
      val cacheParallRddName = getJobResult[String](cacheParallRddJobId)
      println("Jobid = " + cacheParallRddJobId + ",cacheParallRddName = " + cacheParallRddName)

      val useCacheParallRddJobId = cupidSparkClient.startJob("com.aliyun.odps.cupid.client.spark.UseCacheRdd", jarName, configArgs)
      try {
        val useCacheRddResult = getJobResult[Array[Int]](useCacheParallRddJobId)
        println("Jobid = " + useCacheParallRddJobId + ",The useCacheRddJob resutl is ")
        useCacheRddResult.foreach(println)
        assert(useCacheRddResult.length == 3)
      } catch {
        case e: Exception => {
          println("Jobid = " + useCacheParallRddJobId + ", stacktrace is:")
          e.printStackTrace()
          throw e
        }
      }
    }
  }


  class SimpleJob(cupidSparkClient: CupidSparkClientRunner, jarName: String) extends Runnable {
    def run() {
      val configArgs = new HashMap[String, String]()
      configArgs.put("partitionNum", "2")
      val jobId = cupidSparkClient.startJob("com.aliyun.odps.cupid.client.spark.ParallRddJob", jarName, configArgs)
      try {
        val jobResult = getJobResult[Long](jobId)
        println("Jobid = " + jobId + ",job result is " + jobResult)
        assert(jobResult == 11)
      } catch {
        case e: Exception => {
          println("Jobid = " + jobId + ", stacktrace is:")
          e.printStackTrace()
          throw e
        }
      }
    }
  }

  class KillJob(cupidSparkClient: CupidSparkClientRunner, jarName: String) extends Runnable {
    def run() {
      val jobId = cupidSparkClient.startJob("com.aliyun.odps.cupid.client.spark.SparkJobKill", jarName, new util.HashMap[String, String])
      cupidSparkClient.killJob(jobId)
      try {
        getJobResult[Unit](jobId)
      } catch {
        case e: Exception => {
          println("Jobid = " + jobId + ", stacktrace is:")
          e.printStackTrace()
        }
      }
    }
  }
}
