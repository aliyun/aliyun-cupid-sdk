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

trait StatusMessage {
  val jobId: String
}

/**
  * The SparkJob status type
  */
object JobStatus {

  case class JobStart(jobId: String) extends StatusMessage

  case class JobRunning(jobId: String) extends StatusMessage

  case class JobSuccess(jobId: String, result: Array[Byte]) extends StatusMessage

  case class JobFailed(jobId: String, msg: String) extends StatusMessage

  case class JobKilled(jobId: String) extends StatusMessage
}
