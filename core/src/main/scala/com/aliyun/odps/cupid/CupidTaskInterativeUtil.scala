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

import apsara.odps.cupid.protocol.CupidTaskParamProtos.{CupidTaskOperator, CupidTaskParam, SaveTableInfo}
import com.aliyun.odps.cupid.requestcupid.{CupidTaskOperatorConst, CupidTaskRunningMode, SubmitJobUtil}
import org.apache.log4j.Logger

object CupidTaskInterativeUtil {
  val logger = Logger.getLogger(this.getClass().getName())

  def getSaveTempDirAndCapFile(lookupName: String,
                               projectName: String,
                               tableName: String,
                               saveId: String,
                               priority: Int = 9): (String, String) = {
    val moyeTaskParamBuilder = CupidTaskParam.newBuilder()
    val moyeTaskOperatorBuilder = CupidTaskOperator.newBuilder()
    moyeTaskOperatorBuilder.setMoperator(CupidTaskOperatorConst.CUPID_TASK_GET_SAVETEMPDIR)
    moyeTaskOperatorBuilder.setMlookupName(CupidUtil.getEngineLookupName())
    moyeTaskParamBuilder.setMcupidtaskoperator(moyeTaskOperatorBuilder.build())

    val saveTableInfoBuilder = SaveTableInfo.newBuilder()
    saveTableInfoBuilder.setProjectname(projectName)
    saveTableInfoBuilder.setTablename(tableName)
    saveTableInfoBuilder.setSaveid(saveId)
    moyeTaskParamBuilder.setSaveTableInfo(saveTableInfoBuilder.build())

    val moyeTaskParam = moyeTaskParamBuilder.build()
    val instance = SubmitJobUtil.submitJob(moyeTaskParam, CupidTaskRunningMode.eAsyncNotFuxiJob, Some(priority))
    logger.info("get SaveTempDirAndCapFile instance:" + instance.getId())
    val SaveTempDirAndCapFileResult = CupidUtil.pollSuccessResult(instance, "get save project temp dir...").split(",")
    (SaveTempDirAndCapFileResult(0), SaveTempDirAndCapFileResult(1))
  }
}
