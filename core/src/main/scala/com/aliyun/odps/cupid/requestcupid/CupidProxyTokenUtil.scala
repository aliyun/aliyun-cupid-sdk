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

import apsara.odps.cupid.protocol.CupidTaskParamProtos._
import com.aliyun.odps.cupid.CupidSession
import org.apache.log4j.Logger

/**
  * To support threadSafe CupidSession, all help function need to pass cupidSession: CupidSession which is not static
  */
object CupidProxyTokenUtil {

  val logger = Logger.getLogger(this.getClass().getName())

  /**
    * CupidSession thread-safe call
    *
    * @param instanceId
    * @param appName
    * @param expiredInHours
    * @param cupidSession
    * @return
    */
  def getProxyToken(instanceId: String, appName: String, expiredInHours: Int, cupidSession: CupidSession): String = {
    val cupidTaskParamBuilder =
      CupidTaskBaseUtil.getOperationBaseInfo(cupidSession, CupidTaskOperatorConst.CUPID_TASK_GET_PROXY_TOKEN)
    val cupidProxyTokenRequest = CupidProxyTokenRequest.newBuilder()
    cupidProxyTokenRequest.setInstanceId(instanceId)
    // TODO appName is for K8S, please use this parameter after done with K8S Meta
    // cupidProxyTokenRequest.setAppName(appName)
    cupidProxyTokenRequest.setExpiredInHours(expiredInHours)
    cupidTaskParamBuilder.setCupidProxyTokenRequest(cupidProxyTokenRequest.build())
    SubmitJobUtil.getProxyTokenSubmitJob(cupidTaskParamBuilder.build(), cupidSession)
  }
}
