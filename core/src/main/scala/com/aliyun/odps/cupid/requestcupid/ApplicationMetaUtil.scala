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
object ApplicationMetaUtil {

  val logger = Logger.getLogger(this.getClass().getName())

  /**
    * CupidSession thread-safe call
    *
    * @param applicationType
    * @param applicationId
    * @param instanceId
    * @param applicationTags
    * @param applicationName
    * @param cupidSession
    */
  def createApplicationMeta(applicationType: String,
                            applicationId: String,
                            instanceId: String,
                            applicationTags: String,
                            applicationName: String,
                            cupidSession: CupidSession): Unit = {
    val cupidTaskParamBuilder =
      applicationOperationBaseInfo(cupidSession, CupidTaskOperatorConst.CUPID_TASK_CREATE_APPLICATION_META)

    val createApplicationMetaInfo = CreateApplicationMetaInfo.newBuilder()
    createApplicationMetaInfo.setApplicationId(applicationId)
    createApplicationMetaInfo.setApplicationTags(applicationTags)
    createApplicationMetaInfo.setInstanceId(instanceId)
    createApplicationMetaInfo.setApplicationType(applicationType)
    createApplicationMetaInfo.setRunningMode(cupidSession.conf.get("odps.cupid.engine.running.type", "default"))
    createApplicationMetaInfo.setApplicationName(applicationName)

    cupidTaskParamBuilder.setCreateApplicationMetaInfo(createApplicationMetaInfo.build())
    SubmitJobUtil.createApplicationMetaSubmitJob(cupidTaskParamBuilder.build(), cupidSession)
  }

  /**
    * CupidSession thread-safe call
    *
    * @param applicationId
    * @param cupidSession
    * @return
    */
  def getApplicationMeta(applicationId: String, cupidSession: CupidSession): ApplicationMeta = {
    val cupidTaskParamBuilder =
      applicationOperationBaseInfo(cupidSession, CupidTaskOperatorConst.CUPID_TASK_GET_APPLICATION_META)
    val getApplicationMetaInfo = GetApplicationMetaInfo.newBuilder()
    getApplicationMetaInfo.setApplicationId(applicationId)
    cupidTaskParamBuilder.setGetApplicationMetaInfo(getApplicationMetaInfo.build())
    SubmitJobUtil.getApplicationMetaSubmitJob(cupidTaskParamBuilder.build(), cupidSession)
  }

  /**
    * CupidSession thread-safe call
    *
    * @param applicationTypes      A comma-separated applicationType like spark,hive,other
    * @param yarnApplicationStates A comma-separated yarnApplicationStates like 0,1,7,other
    * @param cupidSession
    * @return
    */
  def listApplicationMeta(applicationTypes: String,
                          yarnApplicationStates: String,
                          cupidSession: CupidSession): ApplicationMetaList = {
    val cupidTaskParamBuilder =
      applicationOperationBaseInfo(cupidSession, CupidTaskOperatorConst.CUPID_TASK_LIST_APPLICATION_META)
    val listApplicationMetaInfo = ListApplicationMetaInfo.newBuilder()
    if (applicationTypes != null && applicationTypes != "") {
      listApplicationMetaInfo.setApplicationTypes(applicationTypes)
    }

    if (yarnApplicationStates != null && yarnApplicationStates != "") {
      listApplicationMetaInfo.setYarnApplicationStates(yarnApplicationStates)
    }

    cupidTaskParamBuilder.setListApplicationMetaInfo(listApplicationMetaInfo.build())
    SubmitJobUtil.listApplicationMetaSubmitJob(cupidTaskParamBuilder.build(), cupidSession)
  }

  /**
    * applicationMeta BaseInfo
    * CupidSession thread-safe call
    *
    * @param cupidSession
    * @param cupidTaskOperator
    * @return
    */
  private def applicationOperationBaseInfo(cupidSession: CupidSession, cupidTaskOperator: String): CupidTaskParam.Builder = {
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

  /**
    * CupidSession thread-safe call
    *
    * @param applicationId
    * @param cupidSession
    * @return
    */
  def updateApplicationMeta(applicationId: String,
                            applicationMeta: ApplicationMeta,
                            cupidSession: CupidSession): Unit = {
    val cupidTaskParamBuilder =
      applicationOperationBaseInfo(cupidSession, CupidTaskOperatorConst.CUPID_TASK_UPDATE_APPLICATION_META)
    val updateApplicationMetaInfo = UpdateApplicationMetaInfo.newBuilder()
    updateApplicationMetaInfo.setApplicationId(applicationId)
    updateApplicationMetaInfo.setApplicationMeta(applicationMeta)

    cupidTaskParamBuilder.setUpdateApplicationMetaInfo(updateApplicationMetaInfo.build())
    SubmitJobUtil.updateApplicationMetaSubmitJob(cupidTaskParamBuilder.build(), cupidSession)
  }
}
