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

package com.aliyun.odps.cupid.requestcupid

import org.apache.log4j.Logger

/**
  * functions need retry
  * 1 uploadRes : upload Resource   Method:uploadRes()
  * 2 getLogViewUrl : get LogViewUrl   Method:getLogViewUrl()
  * 3 getTaskDetailJson : get TaskDetailJson   Method:getTaskDetailJson()
  */
object RetryUtil {

  val logger = Logger.getLogger(this.getClass().getName())

  /**
    * This function is used for retry f:()=>W RetryTimes times,if failed then print logInfo(
    * "The " + retryCount + " attempt to " + Message + " failed")
    * the Unit of IntervalTimes is Seconds
    */
  def retryFunction[T](f: () => T, Message: String, RetryTimes: Int = 0, IntervalTimes: Int = 1): T = {
    var tryCount = 1
    val tryTimes = RetryTimes + 1
    while (tryCount <= tryTimes) {
      logger.debug("The " + tryCount + " attempting to " + Message)
      try {
        val result = f()
        logger.debug("The " + tryCount + " attempt to " + Message + " successed")
        return result
      } catch {
        case ie: InterruptedException =>
          logger.error("The " + tryCount + " attempt to " + Message + " failed. " + ie.toString(), ie)
          throw ie
        case er: Error =>
          logger.error("The " + tryCount + " attempt to " + Message + " failed. " + er.toString(), er)
          throw er
        case e: Throwable =>
          logger.info("The " + tryCount + " attempt to " + Message + " failed. " + e.toString())
          if (tryCount >= tryTimes) {
            throw e
          } else {
            tryCount += 1
            Thread.sleep(IntervalTimes * 1000)
          }
      }
    }
    throw new IllegalArgumentException("RetryTimes can not small than 0!")
  }

  object RetryConst {
    //Method:SubmitJobUtil.uploadRes()
    val UPLOAD_RESOURCE = "upload resource"
    //Method:AppRunner.getLogViewUrl()
    val GET_LOG_VIEW_URL = "get logViewUrl"
    //Method:getTaskDetailJson()
    val GET_TASK_DETAIL_JSON = "get TaskDetailJson"
  }
}