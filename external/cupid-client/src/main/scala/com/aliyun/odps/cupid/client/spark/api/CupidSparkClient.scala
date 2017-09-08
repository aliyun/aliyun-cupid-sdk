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

package com.aliyun.odps.cupid.client.spark.api

import java.util.Map

/**
  * Use to communicate with remote dirver
  */
trait CupidSparkClient {
  /**
    * Add the local jar file ,which contains user SparkJobs
    *
    * @param localFilePath the local jar file path
    * @return return the jarName ,the startjob() will use
    */
  def addJar(localFilePath: String): String

  /**
    * After add the jar,can start the sparkjob in the jar
    *
    * @param className the class name in the jar
    * @param jarName   jar name return from the addJar()
    * @param conf      the conf when sparkjob run need
    * @return the jobId, getJobStatus/killJob will use
    */
  def startJob(className: String, jarName: String, conf: Map[String, String]): String

  /**
    * get the jobstatus after the job start
    *
    * @param jobId jobId return from the startJob()
    * @return the job status ,eg: JobStart,JobSuccess,JobFailed,JobKilled
    */
  def getJobStatus(jobId: String): Any

  /**
    * stop the remote driver,then can not submit sparkjob
    */
  def stopRemoteDriver()

  /**
    * kill the sparkjob running
    *
    * @param jobId the jobid will kill
    */
  def killJob(jobId: String)
}
