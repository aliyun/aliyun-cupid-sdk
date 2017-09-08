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

package com.aliyun.odps.cupid.client.spark.service

import java.io._
import java.net.{URI, URL}
import java.text.SimpleDateFormat
import java.util

import apsara.odps.cupid.client.protocol.SparkClient
import apsara.odps.cupid.client.protocol.SparkClient._
import com.aliyun.odps.cupid.client.spark.service.JarInfoManager.JarInfo
import com.aliyun.odps.cupid.client.spark.util.JarUtils
import com.google.protobuf.{RpcCallback, RpcController}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger

import scala.concurrent.Future
import scala.util.{Failure, Success}

case class IllegalJarException(jarName: String, message: String) extends Exception("jarName=" + jarName + "," + message)

class SparkClientServiceImpl(sparkServiceContextImpl: SparkServiceContextImpl, cupidSparkRemoteDriver: CupidSparkRemoteDriver) extends SparkClient.SparkClientService {
  private val logger = Logger.getLogger(this.getClass().getName())
  private val checkHeartBeatThread = new CheckHeartbeatThread
  private val clientTimeout = getClientTimeout
  private var isStop = false
  @volatile private var lastHeartBeatTime = System.currentTimeMillis()

  def getClientTimeout: Int = {
    val timeout = sparkServiceContextImpl.getJobContext().sc().getConf.getInt("odps.cupid.clientmode.heartbeat.timeout", 30 * 60 * 1000)
    logger.info("clientTimeout=" + timeout)
    timeout
  }

  startHeartBeatThread

  def startHeartBeatThread(): Unit = {
    checkHeartBeatThread.setDaemon(true)
    if (checkHeartBeatThread.getState.equals(Thread.State.NEW)) {
      this.updateHeartBeatTime
      checkHeartBeatThread.start()
    }
  }


  override def stopRemoteDriver(controller: RpcController, request: Void, done: RpcCallback[Void]): Unit = {
    cupidSparkRemoteDriver.shutdownDriver()
    isStop = true
  }

  def this() {
    this(null, null)
  }

  override def clientHeartBeat(controller: RpcController, request: Void, done: RpcCallback[Void]): Unit = {
    this.updateHeartBeatTime
  }

  private def updateHeartBeatTime(): Unit = {
    this.lastHeartBeatTime = System.currentTimeMillis()
  }

  override def checkServiceReady(controller: RpcController, request: CheckServiceReadyRequest, done: RpcCallback[CheckServiceReadyResponse]): Unit = {
    val message: String = request.getMessage
    logger.info("The request message is " + message)
    val echoResponseBuilder: SparkClient.CheckServiceReadyResponse.Builder = SparkClient.CheckServiceReadyResponse.newBuilder
    echoResponseBuilder.setResponse("ready")
    done.run(echoResponseBuilder.build)
  }

  override def addJar(controller: RpcController, request: AddJarRequest, done: RpcCallback[Void]): Unit = {
    val panguRelativePath: String = request.getPanguRelativePath
    val tempResourceName: String = request.getTempResourceName
    val panguJobTempRootDir: String = System.getenv("FUXI_JOB_TEMP_ROOT")
    val jarPanguPath: String = panguJobTempRootDir + "/../../" + panguRelativePath
    val localPath = SparkServiceConst.JarLocalDir + "/" + tempResourceName
    val localJarAbsolutePath = new java.io.File(localPath).getAbsolutePath()
    val jarInfo = JarInfo(tempResourceName, localJarAbsolutePath, jarPanguPath)
    logger.info("Now AddJar" + jarInfo.toString)
    this.downloadJar(localPath, jarPanguPath)

    this.sparkServiceContextImpl.getJarInfoManager().addJarInfo(jarInfo.jarName, jarInfo)
    //add the jar to localclassloader and sparkcontext
    this.sparkServiceContextImpl.getJobContext().sc().addJar(jarInfo.jarPanguPath)
    this.sparkServiceContextImpl.classLoader().addURL(new URL("file:" + jarInfo.jarLocalPath))
  }

  private def downloadJar(localPath: String, jarPanguPath: String): Unit = {
    val hadoopConf = new Configuration()
    hadoopConf.set("volume.internal", "true")
    hadoopConf.set("fs.odps.impl", "com.aliyun.odps.fs.volume.InternalVolumeFileSystem")
    hadoopConf.set(
      "fs.AbstractFileSystem.odps.impl",
      "com.aliyun.odps.fs.volume.abstractfsimpl.InternalVolumeFs")
    hadoopConf.set("fs.pangu.impl", "com.aliyun.odps.fs.hadoop.HadoopPanguFileSystem")
    hadoopConf.setBoolean("pangu.access.check", false)
    val jarUri = new URI(jarPanguPath)
    val jarPath = new Path(jarUri)
    val panguFS = FileSystem.get(jarUri, hadoopConf)
    if (!panguFS.exists(jarPath)) {
      throw new IOException(s"The jar not exist in pangu,jarName = $localPath")
    }

    val panguIn = panguFS.open(jarPath)
    val localOutFile = new File(localPath)
    if (localOutFile.exists()) {
      logger.info(s"The local file is exist,file is $localOutFile")
      return
    }
    val localOutStream = new FileOutputStream(localOutFile)
    try {
      val buf = new Array[Byte](8192)
      var n = 0
      while (n != -1) {
        n = panguIn.read(buf)
        if (n != -1) {
          localOutStream.write(buf, 0, n)
        }
      }
    } finally {
      panguIn.close()
      localOutStream.close()
    }
  }

  override def startJob(controller: RpcController, request: StartJobRequest, done: RpcCallback[StartJobResponse]): Unit = {

    val executionContext = this.sparkServiceContextImpl.getExecutionContext()
    val className: String = request.getClassName
    val jobId = getTheClassName(className) + "_" + java.util.UUID.randomUUID().toString()
    this.sparkServiceContextImpl.getJobInfoManager().updateStartJobStatus(jobId)
    Future {
      try {
        Thread.currentThread().setContextClassLoader(this.sparkServiceContextImpl.classLoader())
        val jarName: String = request.getJarName
        val jobConf = transformJobConf(request.getJobConf)

        val jarInfo = this.sparkServiceContextImpl.getJarInfoManager().getJarInfo(jarName)
        if (jarInfo == null) {
          throw new IllegalJarException(jarName, "The jar do not exist")
        }

        //get the job constructor
        val constructor = JarUtils.loadClassOrObject[com.aliyun.odps.cupid.client.spark.api.SparkJob](className, this.sparkServiceContextImpl.classLoader())

        logger.info(s"Now startJob jobid=$jobId,className=$className,jarName=$jarName")
        val job = constructor()
        this.sparkServiceContextImpl.getJobContext().sc().setJobGroup(jobId, s"Job group for $jobId and spark context ", true)
        this.sparkServiceContextImpl.getJobInfoManager().updateRunningJobStatus(jobId)
        val result = job.runJob(this.sparkServiceContextImpl.getJobContext(), jobConf)
        result
      } catch {
        case e: Throwable => {
          throw new Exception(getTheStackTraceStr(e))
        };
      }
    }(executionContext).andThen {
      case Success(value: Array[Byte]) => {
        logger.info(s"The job success jobid=$jobId,className=$className")
        this.sparkServiceContextImpl.getJobInfoManager().updateSuccessJobStatus(jobId, value)
      }
      case Failure(e: Throwable) => {
        val errorMsg = s"The job failed jobid=$jobId,className=$className,failedMsg=$e.getMessage"
        logger.error(errorMsg)
        this.sparkServiceContextImpl.getJobInfoManager().updateFailedJobStatus(jobId, errorMsg)
      }
    }(executionContext)
    val startJobResponseBuilder = SparkClient.StartJobResponse.newBuilder()
    startJobResponseBuilder.setJobId(jobId)
    done.run(startJobResponseBuilder.build())
  }

  private def transformJobConf(jobConf: JobConf): Config = {
    import scala.collection.JavaConverters._
    val jobConfMap = new util.HashMap[String, String]
    jobConf.getJobconfitemList.asScala.foreach(jobConfItem => {
      jobConfMap.put(jobConfItem.getKey, jobConfItem.getValue)
    })
    ConfigFactory.parseMap(jobConfMap)
  }

  private def getTheStackTraceStr(e: Throwable): String = {
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    try {
      e.printStackTrace(pw)
      sw.close()
      sw.toString()
    } finally {
      pw.close()
    }

  }

  private def getTheClassName(classFullName: String): String = {
    classFullName.split("\\.").toList.last
  }

  override def getJobStatus(controller: RpcController, request: GetJobStatusRequest, done: RpcCallback[GetJobStatusResponse]): Unit = {
    val jobId = request.getJobId
    val jobStatus = this.sparkServiceContextImpl.getJobInfoManager().getJobStatus(jobId)
    done.run(jobStatus)
  }

  override def killJob(controller: RpcController, request: KillJobRequest, done: RpcCallback[Void]): Unit = {
    val jobId = request.getJobId
    this.sparkServiceContextImpl.getJobContext().sc().cancelJobGroup(jobId)
    this.sparkServiceContextImpl.getJobInfoManager().updateKillJobStatus(jobId)
  }

  private class CheckHeartbeatThread() extends Thread {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    override def run(): Unit = {
      while (!isStop) {
        try {
          Thread.sleep(5 * 60 * 1000)
          val nowTime = System.currentTimeMillis()
          val lastUpdateTime = lastHeartBeatTime
          logger.info("The last update time is " + formatter.format(lastUpdateTime))
          if (nowTime - lastUpdateTime > clientTimeout) {
            logger.warn("The heartbeat has timeout , lastupdatetime is " + formatter.format(lastUpdateTime))
            cupidSparkRemoteDriver.shutdownDriver()
            isStop = true
          }
        } catch {
          case ex: Throwable => logger.warn("The HeartBeatThread interrupted" + ex.getCause)
        }
      }
    }
  }
}
