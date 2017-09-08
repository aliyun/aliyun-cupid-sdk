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
import com.aliyun.odps.cupid.{CupidConf, CupidSession}
import org.apache.log4j.Logger

/**
  * To support threadSafe CupidSession, all help function need to pass cupidSession: CupidSession which is not static
  */
object CupidTaskBaseUtil {

  val logger = Logger.getLogger(this.getClass().getName())

  /**
    * CupidSession thread-safe call
    *
    * @param cupidSession
    * @param cupidTaskOperator
    * @return
    */
  def getOperationBaseInfo(cupidSession: CupidSession, cupidTaskOperator: String): CupidTaskParam.Builder = {
    val moyeTaskParamBuilder = CupidTaskParam.newBuilder()
    val moyeTaskOperatorBuilder = CupidTaskOperator.newBuilder()
    moyeTaskOperatorBuilder.setMoperator(cupidTaskOperator)

    moyeTaskParamBuilder.setMcupidtaskoperator(moyeTaskOperatorBuilder.build())
    moyeTaskParamBuilder.setJobconf(getConfBuilder(cupidSession).build())
    moyeTaskParamBuilder
  }

  /**
    * JobConf builder
    *
    * @param cupidSession
    * @return
    */
  private def getConfBuilder(cupidSession: CupidSession): JobConf.Builder = {
    val appConf: CupidConf = cupidSession.conf
    val jobConfBuilder = JobConf.newBuilder()
    val jobConfItemBuilder = JobConfItem.newBuilder()

    val appConfAll = appConf.getAll
    for (appConfItem <- appConfAll) {
      if (appConfItem._1.startsWith("cupid")
        || appConfItem._1.startsWith("odps.moye")
        || appConfItem._1.startsWith("odps.executor")
        || appConfItem._1.startsWith("odps.cupid")) {
        jobConfItemBuilder.setKey(appConfItem._1)
        jobConfItemBuilder.setValue(appConfItem._2)
        jobConfBuilder.addJobconfitem(jobConfItemBuilder.build())
      }
    }
    jobConfBuilder
  }
}
