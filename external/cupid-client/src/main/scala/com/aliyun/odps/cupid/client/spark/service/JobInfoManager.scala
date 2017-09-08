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

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import apsara.odps.cupid.client.protocol.SparkClient._
import com.google.protobuf.ByteString
import org.apache.log4j.Logger

object JobInfoManager {

  case class JobNotExistException(jobId: String, message: String) extends Exception(message)

  case class IllegalStateException(jobId: String, message: String) extends Exception(message)

}

class JobInfoManager() {
  private val logger = Logger.getLogger(this.getClass().getName())
  private val jobStatus: ConcurrentMap[String, GetJobStatusResponse] = new ConcurrentHashMap[String, GetJobStatusResponse]

  def updateStartJobStatus(jobId: String): Unit = {
    val getJobStatusBuilder = GetJobStatusResponse.newBuilder()
    getJobStatusBuilder.setJobId(jobId)
    val jobStartBuilder = JobStart.newBuilder()
    jobStartBuilder.setJobId(jobId)
    getJobStatusBuilder.setJobStart(jobStartBuilder.build())
    logger.info("update start job status," + "jobid = " + jobId + ",status = " + getJobStatusBuilder.build().toString)
    jobStatus.put(jobId, getJobStatusBuilder.build())
  }

  def updateRunningJobStatus(jobId: String): Unit = {
    val getJobStatusBuilder = GetJobStatusResponse.newBuilder()
    getJobStatusBuilder.setJobId(jobId)
    val jobRunningBuilder = JobRunning.newBuilder()
    jobRunningBuilder.setJobId(jobId)
    getJobStatusBuilder.setJobRunning(jobRunningBuilder.build())
    jobStatus.put(jobId, getJobStatusBuilder.build())
  }

  def updateSuccessJobStatus(jobId: String, result: Array[Byte]): Unit = {
    if (isJobTerminated(jobId)) {
      val theJobStatus = jobStatus.get(jobId)
      if (theJobStatus.hasJobKilled) {
        val msg = "The job success after job be killed," + "jobid = " + jobId
        this.updateFailedJobStatus(jobId, msg)
        return
      } else if (theJobStatus.hasJobFailed) {
        val failMsg = theJobStatus.getJobFailed.getErrormsg
        val msg = "The job success after job failed," + "failMsg = " + failMsg + "jobid = " + jobId
        this.updateFailedJobStatus(jobId, msg)
        return
      }
    }
    val getJobStatusBuilder = GetJobStatusResponse.newBuilder()
    getJobStatusBuilder.setJobId(jobId)
    val jobSuccessBuilder = JobSuccess.newBuilder()
    jobSuccessBuilder.setJobId(jobId)
    val byteStringResult = ByteString.copyFrom(result)
    jobSuccessBuilder.setJobResult(byteStringResult)
    jobSuccessBuilder.setJobId(jobId)
    getJobStatusBuilder.setJobSuccess(jobSuccessBuilder.build())
    jobStatus.put(jobId, getJobStatusBuilder.build())
  }

  def updateFailedJobStatus(jobId: String, errorMsg: String): Unit = {
    if (isJobTerminated(jobId)) return
    val getJobStatusBuilder = GetJobStatusResponse.newBuilder()
    getJobStatusBuilder.setJobId(jobId)
    val jobFailedBuilder = JobFailed.newBuilder()
    jobFailedBuilder.setJobId(jobId)
    jobFailedBuilder.setErrormsg(errorMsg)
    getJobStatusBuilder.setJobFailed(jobFailedBuilder.build())
    jobStatus.put(jobId, getJobStatusBuilder.build())
  }

  def isJobTerminated(jobId: String): Boolean = {
    val theJobStatus = jobStatus.get(jobId)
    if (theJobStatus.hasJobFailed || theJobStatus.hasJobKilled || theJobStatus.hasJobSuccess) return true
    else return false
  }

  def updateKillJobStatus(jobId: String): Unit = {
    if (isJobTerminated(jobId)) return
    val getJobStatusBuilder = GetJobStatusResponse.newBuilder()
    getJobStatusBuilder.setJobId(jobId)
    val jobKilledBuilder = JobKilled.newBuilder()
    jobKilledBuilder.setJobId(jobId)
    getJobStatusBuilder.setJobKilled(jobKilledBuilder.build())
    jobStatus.put(jobId, getJobStatusBuilder.build())
  }

  def getJobStatus(jobId: String): GetJobStatusResponse = {
    import JobInfoManager._
    if (!jobStatus.containsKey(jobId)) {
      logger.info("The job status not exist,jobid=" + jobId)
      throw JobNotExistException(jobId, "the job status not exist")
    } else {
      return jobStatus.get(jobId)
    }
  }
}
