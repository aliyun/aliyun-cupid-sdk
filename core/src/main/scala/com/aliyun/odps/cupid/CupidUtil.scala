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

package com.aliyun.odps.cupid

import apsara.odps.cupid.protocol.CupidTaskParamProtos.{CupidTaskDetailResultParam, Ready}
import com.aliyun.odps.cupid.requestcupid.RetryUtil
import com.aliyun.odps.{Instance, LogView}
import org.apache.commons.codec.binary.Base64
import org.apache.log4j.Logger

object CupidUtil {

  val logger = Logger.getLogger(this.getClass().getName())

  def pollSuccessResult(ins: Instance, msg: String): String = {
    val break = false
    while (!break) {
      val re = getResult(ins)
      if (re.hasFailed()) {
        if (re.getFailed().hasBizFailed()) {
          throw errMsg2SparkException(re.getFailed().getBizFailed().getBizFailedMsg())
        }
      } else if (re.hasCancelled) {
        throw new UserException("instance be cancelled")
      } else if (re.hasRunning()) {
        logger.info(msg)
        Thread.sleep(1000)
      } else if (re.hasSuccess()) {
        return re.getSuccess().getSuccessMsg()
      } else {
        if (re.toString != "") {
          // empty case means the cupid task not set TaskDetail
          logger.info("unexpected status: " + re.toString)
        }

        Thread.sleep(1000)
      }
    }
    return "";
  }

  def getResult(ins: Instance): CupidTaskDetailResultParam = {
    var result: CupidTaskDetailResultParam = null
    var break = false
    while (!break) {
      result = getInsStatus(ins)
      if (result.hasFailed() && result.getFailed().hasCupidTaskFailed()) {
        throw errMsg2SparkException(result.getFailed().getCupidTaskFailed().getCupidTaskFailedMsg())
      } else if (result.hasReady()) {
        logger.info("ready!!!")
        Thread.sleep(1000)
      } else if (result.hasWaiting()) {
        logger.info("waiting!!!")
        Thread.sleep(1000)
      } else {
        break = true
      }
    }
    result
  }

  def errMsg2SparkException(errMsg: String): CupidException = {
    if (errMsg.startsWith("runTask failed:") || errMsg.startsWith("app run failed!")) {
      new UserException(errMsg)
    } else {
      new CupidException(errMsg)
    }
  }

  def getInsStatus(ins: Instance): CupidTaskDetailResultParam = {
    val detailResult = getTaskDetailJson(ins)
    if (detailResult == InstanceRecycledException.InstanceRecycledMsg) {
      throw new InstanceRecycledException(detailResult)
    }

    val taskDetailResultParam = CupidTaskDetailResultParam.parseFrom(Base64.decodeBase64(detailResult.getBytes()))
    if (taskDetailResultParam.hasReady ||
      taskDetailResultParam.hasWaiting ||
      taskDetailResultParam.hasRunning ||
      taskDetailResultParam.hasSuccess ||
      taskDetailResultParam.hasFailed ||
      taskDetailResultParam.hasCancelled
    ) {
      taskDetailResultParam
    }
    else {
      logger.debug("taskDetailResultParam is empty, set Ready!")
      taskDetailResultParam.toBuilder.setReady(Ready.newBuilder().build()).build()
    }
  }

  def getTaskDetailJson(ins: Instance): String = {
    RetryUtil.retryFunction(
      () => {
        ins.getTaskDetailJson("cupid_task")
      },
      RetryUtil.RetryConst.GET_TASK_DETAIL_JSON, 2)
  }

  def getLogViewUrl(instance: Instance): String = {
    val logViewHandler = new LogView(CupidSession.get.odps)
    val logViewUrl: String = RetryUtil.retryFunction(
      () => {
        logViewHandler.generateLogView(instance, 7 * 24)
      },
      RetryUtil.RetryConst.GET_LOG_VIEW_URL, 60)
    logViewUrl
  }

  def getEngineLookupName(): String = {
    CupidSession.get.getJobLookupName
  }
}
