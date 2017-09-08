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

package com.aliyun.odps.cupid.client.spark.client

import java.io.{File, FileInputStream, IOException, InputStreamReader}
import java.util
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.{Map, Properties}

import apsara.odps.cupid.client.protocol.SparkClient
import com.aliyun.odps.Odps
import com.aliyun.odps.cupid.client.rpc.{CupidClientRpcChannel, CupidClientRpcController, CupidHttpRpcChannelProxy, CupidRpcChannelProxy}
import com.aliyun.odps.cupid.client.spark.api.CupidSparkClient
import com.aliyun.odps.cupid.client.spark.api.JobStatus._
import com.aliyun.odps.cupid.client.spark.util.JarUtils
import com.aliyun.odps.cupid.requestcupid.CopyTempResourceUtil
import com.aliyun.odps.cupid.{CupidConf, CupidSession}
import com.google.protobuf.RpcCallback
import org.apache.log4j.Logger
import org.apache.spark.deploy.SparkSubmit
import org.apache.spark.{SparkConf, SparkException}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.util.{Failure, Success}

case class IllegalStatusException(jobId: String, message: String) extends Exception(message)

class CupidSparkClientRunner extends CupidSparkClient {
  private val alreadyAddJars: ConcurrentMap[String, String] = new ConcurrentHashMap[String, String]

  private val logger = Logger.getLogger(this.getClass().getName())
  private var stub: SparkClient.SparkClientService.Stub = null
  private var odps: Odps = null
  private var isStop: Boolean = false
  private var heartbeatMaxFailTimes: Int = 20
  private var heartbeatThread: Thread = null
  private var lookupName = ""

  def getLookupName: String = {
    lookupName
  }

  // for test
  def this(cupidRpcChannelProxy: CupidRpcChannelProxy) {
    this()
    val cupidClientRpcChannel: CupidClientRpcChannel = new CupidClientRpcChannel(cupidRpcChannelProxy)
    stub = SparkClient.SparkClientService.newStub(cupidClientRpcChannel)
  }

  def this(odps: Odps, lookupName: String, heartbeatMaxFailTimes: Int) {
    this()
    this.lookupName = lookupName
    this.heartbeatMaxFailTimes = heartbeatMaxFailTimes
    this.heartbeatThread = new HeartbeatThread
    this.heartbeatThread.setDaemon(true)
    this.odps = odps
    val cupidHttpRpcChannelProxy: CupidHttpRpcChannelProxy = new CupidHttpRpcChannelProxy(odps, lookupName)
    val cupidClientRpcChannel: CupidClientRpcChannel = new CupidClientRpcChannel(cupidHttpRpcChannelProxy)
    stub = SparkClient.SparkClientService.newStub(cupidClientRpcChannel)
  }

  def checkServiceReady(request: String): String = {
    val requestBuilder: SparkClient.CheckServiceReadyRequest.Builder = SparkClient.CheckServiceReadyRequest.newBuilder
    requestBuilder.setMessage(request)
    val clientRpcController: CupidClientRpcController = new CupidClientRpcController
    val checkServiceReadyCallBack: this.CheckServiceReadyCallBack = new this.CheckServiceReadyCallBack
    stub.checkServiceReady(clientRpcController, requestBuilder.build, checkServiceReadyCallBack)
    val checkServiceReadyResponse: SparkClient.CheckServiceReadyResponse = checkServiceReadyCallBack.GetResponse
    if (clientRpcController.failed) {
      throw new IOException(clientRpcController.errorText)
    }
    startHeartBeat
    checkServiceReadyResponse.getResponse
  }

  private def startHeartBeat(): Unit = {
    if (heartbeatThread.getState.equals(Thread.State.NEW)) {
      this.heartbeatThread.start()
      logger.info("Start the heartbeat thread")
    }
  }

  def addJar(localFilePath: String): String = {
    if (alreadyAddJars.containsKey(localFilePath)) {
      logger.warn("The jar already add,jar = " + localFilePath)
      return alreadyAddJars.get(localFilePath)
    }
    val addJarInfo: (String, String) = CopyTempResourceUtil.addAndCopyTempResource(localFilePath, this.odps)
    val requestBuilder: SparkClient.AddJarRequest.Builder = SparkClient.AddJarRequest.newBuilder
    val jarName = addJarInfo._1
    requestBuilder.setPanguRelativePath(addJarInfo._2)
    requestBuilder.setTempResourceName(addJarInfo._1)
    val clientRpcController: CupidClientRpcController = new CupidClientRpcController
    val callback = new RpcCallback[SparkClient.Void]() {
      def run(message: SparkClient.Void) {
      }
    }
    stub.addJar(clientRpcController, requestBuilder.build, callback)
    if (clientRpcController.failed) {
      throw new IOException(clientRpcController.errorText)
    }
    jarName
  }

  def startJob(className: String, jarName: String, conf: Map[String, String]): String = {
    val requestBuilder: SparkClient.StartJobRequest.Builder = SparkClient.StartJobRequest.newBuilder
    requestBuilder.setClassName(className)
    requestBuilder.setJarName(jarName)
    val jobConfBuilder = SparkClient.JobConf.newBuilder()
    val jobConfItemBuilder = SparkClient.JobConfItem.newBuilder()
    import scala.collection.JavaConverters._
    conf.asScala.foreach(confItem => {
      jobConfItemBuilder.setKey(confItem._1)
      jobConfItemBuilder.setValue(confItem._2)
      jobConfBuilder.addJobconfitem(jobConfItemBuilder.build())
    })
    requestBuilder.setJobConf(jobConfBuilder.build())

    val jobId: StringBuilder = new StringBuilder
    val clientRpcController: CupidClientRpcController = new CupidClientRpcController
    val callback = new RpcCallback[SparkClient.StartJobResponse]() {
      def run(message: SparkClient.StartJobResponse) {
        jobId.append(message.getJobId)
      }
    }
    stub.startJob(clientRpcController, requestBuilder.build, callback)
    if (clientRpcController.failed) {
      throw new IOException(clientRpcController.errorText)
    }
    jobId.toString
  }

  def killJob(jobId: String) = {
    val killJobRequestBuilder: SparkClient.KillJobRequest.Builder = SparkClient.KillJobRequest.newBuilder
    killJobRequestBuilder.setJobId(jobId)
    val clientRpcController: CupidClientRpcController = new CupidClientRpcController
    val callback = new RpcCallback[SparkClient.Void]() {
      def run(message: SparkClient.Void) {
      }
    }
    //wait for the job has submit to spark cluster
    this.waitForJobRunning(jobId: String) match {
      case JobRunning(jobId) => logger.info(s"jobid = $jobId,is running, now kill it")
      case _ => logger.info(s"jobid = $jobId,is already end")
    }
    Thread.sleep(1000)
    stub.killJob(clientRpcController, killJobRequestBuilder.build, callback)
    if (clientRpcController.failed) {
      throw new IOException(clientRpcController.errorText)
    }
  }

  private def waitForJobRunning(jobId: String): Any = {
    var jobEnd = false
    var jobStatus: Any = None
    while (!jobEnd) {
      jobStatus = getJobStatus(jobId)
      jobStatus match {
        case JobStart(jobId) => {
          Thread.sleep(1500)
          logger.info(s"jobid =$jobId ,job status is jobstart")
        }
        case _ => jobEnd = true
      }
    }
    jobStatus
  }

  def getJobResult(jobId: String): Any = {
    var jobEnd = false
    var jobStatus: Any = None
    while (!jobEnd) {
      jobStatus = getJobStatus(jobId)
      jobStatus match {
        case JobStart(jobId) => {
          Thread.sleep(1500)
          logger.info(s"jobid =$jobId ,job status is jobstart")
        }
        case JobRunning(jobId) => {
          Thread.sleep(1500)
          logger.info(s"jobid =$jobId ,job status is running")
        }
        case _ => jobEnd = true
      }
    }
    jobStatus
  }

  def getJobStatus(jobId: String): Any = {
    var retryTime = 2
    while (retryTime >= 0) {
      val requestBuilder: SparkClient.GetJobStatusRequest.Builder = SparkClient.GetJobStatusRequest.newBuilder
      requestBuilder.setJobId(jobId)
      val clientRpcController: CupidClientRpcController = new CupidClientRpcController
      val callback = new RpcCallback[SparkClient.GetJobStatusResponse]() {
        private var response: SparkClient.GetJobStatusResponse = null

        def getResponse: SparkClient.GetJobStatusResponse = {
          return this.response
        }

        def run(parameter: SparkClient.GetJobStatusResponse) {
          this.response = parameter
        }
      }
      stub.getJobStatus(clientRpcController, requestBuilder.build, callback)
      if (clientRpcController.failed) {
        if (retryTime == 0)
          throw new IOException(clientRpcController.errorText)
        else retryTime = retryTime - 1
      } else {
        val jobStatusResponse = callback.getResponse
        return this.parseJobStatus(jobId, jobStatusResponse)
      }
    }
  }

  private def parseJobStatus(jobId: String, jobStatusResponse: SparkClient.GetJobStatusResponse): Any = {
    if (jobStatusResponse.getJobId != jobId) {
      val msg = "ResquestJobId=" + jobId + ",responseJobId=" + jobStatusResponse.getJobId + ",the send and response jobid not equal"
      logger.error("In parseJobStatus:" + msg)
      throw new IllegalStatusException(jobId, msg)
    }
    if (jobStatusResponse.hasJobFailed) {
      val jobInfoFailed = jobStatusResponse.getJobFailed
      JobFailed(jobInfoFailed.getJobId, jobInfoFailed.getErrormsg)
    } else if (jobStatusResponse.hasJobStart) {
      val jobInfoStart = jobStatusResponse.getJobStart
      JobStart(jobInfoStart.getJobId)
    } else if (jobStatusResponse.hasJobSuccess) {
      val jobInfoSuccess = jobStatusResponse.getJobSuccess
      JobSuccess(jobInfoSuccess.getJobId, jobInfoSuccess.getJobResult.toByteArray)
    } else if (jobStatusResponse.hasJobKilled) {
      val jobInfoKilled = jobStatusResponse.getJobKilled
      JobKilled(jobInfoKilled.getJobId)
    } else if (jobStatusResponse.hasJobRunning) {
      val jobInfoRunning = jobStatusResponse.getJobRunning
      JobRunning(jobInfoRunning.getJobId)
    } else {
      val msg = "JobId=" + jobId + "The job status can not identify" + jobStatusResponse.toString
      logger.error("In parseJobStatus:" + msg)
      throw new IllegalStatusException(jobId, msg)
    }
  }

  override def stopRemoteDriver(): Unit = {
    isStop = true
    val requestBuilder: SparkClient.Void.Builder = SparkClient.Void.newBuilder()
    val clientRpcController: CupidClientRpcController = new CupidClientRpcController
    val callback = new RpcCallback[SparkClient.Void]() {
      def run(message: SparkClient.Void) {
      }
    }
    stub.stopRemoteDriver(clientRpcController, requestBuilder.build(), callback)
    if (clientRpcController.failed) {
      throw new IOException(clientRpcController.errorText)
    }
    logger.info("Now stop the remote driver,lookupname is %s".format(CupidSession.get.getJobLookupName))
    CupidSession.reset
  }

  private def heartBeatToDriver(): Unit = {
    val requestBuilder: SparkClient.Void.Builder = SparkClient.Void.newBuilder()
    val clientRpcController: CupidClientRpcController = new CupidClientRpcController
    val callback = new RpcCallback[SparkClient.Void]() {
      def run(message: SparkClient.Void) {
      }
    }
    stub.clientHeartBeat(clientRpcController, requestBuilder.build(), callback)
    if (clientRpcController.failed) {
      logger.warn("In heartBeatToDriver " + clientRpcController.errorText())
      throw new IOException(clientRpcController.errorText)
    }
  }

  private class CheckServiceReadyCallBack extends RpcCallback[SparkClient.CheckServiceReadyResponse] {
    private var response: SparkClient.CheckServiceReadyResponse = null

    def GetResponse: SparkClient.CheckServiceReadyResponse = {
      return this.response
    }

    def run(parameter: SparkClient.CheckServiceReadyResponse) {
      this.response = parameter
    }
  }

  private class HeartbeatThread extends Thread {
    var heartbeatFailTimes = 0

    override def run(): Unit = {
      while (!isStop) {
        try {
          Thread.sleep(10 * 60 * 1000)
          heartBeatToDriver()
        } catch {
          case ex: Throwable => {
            heartbeatFailTimes += 1
            logger.warn("The HeartBeatThread exception" + ex.getCause)
            if (heartbeatFailTimes == heartbeatMaxFailTimes) {
              throw ex
            }
          }
        }
      }
    }
  }

}

object CupidSparkClientRunner {
  private val logger = Logger.getLogger(this.getClass().getName())

  /**
    * use lookupname to generate the cupidsparkclient,then manage the jobs
    *
    * @param lookupName the remoteserver lookupname
    * @param userConf   the odps
    * @return the cupidsparkclient
    */
  def getSparkClientWithLookupName(lookupName: String, userConf: Map[String, String] = new util.HashMap[String, String]()): CupidSparkClientRunner = {
    val sparkHome = Option(System.getProperty("spark.home")).getOrElse(System.getenv("SPARK_HOME"))
    val propertiesFile = {
      if (sparkHome != null) {
        Option(userConf.get("spark.default.conf")).getOrElse(sparkHome + "/conf/spark-defaults.conf")
      } else {
        Option(userConf.get("spark.default.conf")).getOrElse("")
      }
    }
    val sparkConf = getSparkConf(propertiesFile)
    userConf.foreach(item => {
      sparkConf.set(item._1, item._2)
    })

    val cupidConf = new CupidConf()
    sparkConf.getAll.foreach(confIterm => {
      cupidConf.set(confIterm._1, confIterm._2)
    })
    CupidSession.reset
    CupidSession.setConf(cupidConf)
    CupidSession.get.setJobLookupName(lookupName)
    val odps = CupidSession.get.odps

    val cupidSparkClient = new CupidSparkClientRunner(odps, lookupName, cupidConf.get("odps.cupid.spark.client.heartbeat.maxfailtimes", "20").toInt)
    cupidSparkClient.startHeartBeat()
    cupidSparkClient
  }

  private def getSparkConf(propertiesFile: String): SparkConf = {
    if (propertiesFile != "") {
      val defaultProperties = new util.HashMap[String, String]()
      Option(propertiesFile).foreach { filename =>
        getPropertiesFromFile(filename).foreach { case (k, v) =>
          defaultProperties(k) = v
        }
      }
      if (defaultProperties.size == 0)
        throw new IOException("your propertiesFile is empty")
      for ((key, value) <- defaultProperties) {
        System.setProperty(key, value)
      }
    }
    new SparkConf()
  }

  /** Load properties present in the given file. */
  private def getPropertiesFromFile(filename: String): scala.collection.Map[String, String] = {
    val file = new File(filename)
    require(file.exists(), s"Properties file $file does not exist")
    require(file.isFile(), s"Properties file $file is not a normal file")
    val inReader = new InputStreamReader(new FileInputStream(file), "UTF-8")
    try {
      val properties = new Properties()
      properties.load(inReader)
      properties.stringPropertyNames().map(k => (k, properties(k).trim)).toMap
    } catch {
      case e: IOException =>
        throw new SparkException(s"Failed when loading Spark properties from $filename", e)
    } finally {
      inReader.close()
    }
  }

  def getReadyCupidSparkClient(userConf: Map[String, String]): CupidSparkClientRunner = {
    val args = new ArrayBuffer[String]()
    userConf.foreach {
      case (k, v) => {
        args += "--conf"
        args += s"$k=$v"
      }
    }
    getReadyCupidSparkClient(args.toArray)
  }

  //User can specify sparkSubmitArgs, with which the spark cluster will start.
  def getReadyCupidSparkClient(sparkSubmitArgs: Array[String] = Array[String]()): CupidSparkClientRunner = {
    CupidSession.reset
    startSparkCluster(sparkSubmitArgs)
    if (!CupidSession.get.getJobRunning()) {
      val msg = "start spark cluster timeout, you can set odps.cupid.wait.am.start.time(600) to wait longer."
      logger.fatal(msg)
      stop()
      throw new IOException(msg)
    }
    val heartbeatMaxFailTimes = CupidSession.getConf.get("odps.cupid.spark.client.heartbeat.maxfailtimes", "20").toInt
    val cupidSparkClient: CupidSparkClientRunner = new CupidSparkClientRunner(CupidSession.get.odps, CupidSession.get.getJobLookupName, heartbeatMaxFailTimes)
    val requestBuilder: SparkClient.CheckServiceReadyRequest.Builder = SparkClient.CheckServiceReadyRequest.newBuilder
    requestBuilder.setMessage("hello")
    var ready = false
    val begin = System.currentTimeMillis()
    val waitTime = CupidSession.getConf.get("odps.cupid.spark.client.ready.timeout", "60").toInt * 1000 + begin
    var exception: Throwable = null
    while (ready == false && waitTime > System.currentTimeMillis()) {
      try {
        cupidSparkClient.checkServiceReady("check service")
        ready = true
      } catch {
        case e: IOException => {
          exception = e
          logger.info("waiting sparkservice to be ready.")
          Thread.sleep(1000)
        }
      }
    }
    if (!ready) {
      val last = System.currentTimeMillis() - begin
      logger.error("spark service not ready after " + last + "ms, kill the odps instance now.", exception)
      stop()
      throw new WaitSparkReadyException("", exception)
    }
    cupidSparkClient
  }

  private def startSparkCluster(userArgs: Array[String]): Unit = {
    val finalArgs = sanitizeSparkClusterArgs(userArgs)
    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      try {
        SparkSubmit.main(finalArgs)
      } catch {
        case e: Throwable => {
          throw new Exception(e.getMessage)
        };
      }
    } onComplete {
      case Failure(e: Throwable) => {
        val errorMsg = "start sparksubmit main failed " + e.getMessage
        logger.error(errorMsg)
        throw new Exception(errorMsg)
      }
      case Success(result: Any) => {
        logger.info("The remotedriver start ok, result = %s".format(result))
      }
    }
  }

  private def sanitizeSparkClusterArgs(userArgs: Array[String]): Array[String] = {
    val argsNow: ArrayBuffer[String] = ArrayBuffer[String]()
    val sparkHome = Option(System.getProperty("spark.home")).getOrElse(System.getenv("SPARK_HOME"))

    //set default args
    if (sparkHome != null) {
      //1.set the default user conf
      argsNow += "--properties-file"
      argsNow += Option(System.getProperty("spark.default.conf")).getOrElse(sparkHome + "/conf/spark-defaults.conf")

      //      //2.set the default spark jar
      //      new File(sparkHome + "/lib/").listFiles().filter(file => {
      //        file.getName.startsWith("spark-assembly")
      //      }).map(sparkAssemblyJar => {
      //        logger.info("spark assembly jar: " + sparkAssemblyJar.getAbsolutePath)
      //        argsNow += "--conf"
      //        argsNow += "spark.yarn.jar=" + sparkAssemblyJar.getAbsolutePath
      //      })
    }

    //2. add user args
    argsNow ++= userArgs

    //3. set the yarn mode
    argsNow += "--master"
    argsNow += "yarn-cluster"

    //4.set the remoteserver class
    argsNow += "--class"
    argsNow += "com.aliyun.odps.cupid.client.spark.service.CupidSparkRemoteDriver"

    //5. add the sparkclient jar as the cluster user jar
    val clientJarPath = JarUtils.jarOfClass(this.getClass).getOrElse({
      throw new IllegalArgumentException("spark client jar not found.")
    })
    argsNow += clientJarPath

    logger.info("The final spark cluster start args: " + argsNow.mkString(" "))
    argsNow.toArray
  }

  private def stop(): Unit = {
    val instanceId = CupidSession.get.getJobLookupName
    if (!"".equals(instanceId)) {
      CupidSession.get.odps.instances().get(instanceId).stop()
    }
  }

  case class WaitSparkReadyException(val message: String, val cause: Throwable) extends Exception(message, cause)

}
