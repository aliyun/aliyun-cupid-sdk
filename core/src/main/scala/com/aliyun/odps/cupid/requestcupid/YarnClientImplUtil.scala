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

import java.util.{HashMap, Random}

import apsara.odps.cupid.protocol.CupidTaskParamProtos
import apsara.odps.cupid.protocol.CupidTaskParamProtos._
import apsara.odps.cupid.protocol.YarnClientProtos._
import com.aliyun.odps.Instance
import com.aliyun.odps.cupid.util.TrackUrl
import com.aliyun.odps.cupid.utils.SDKConstants
import com.aliyun.odps.cupid.{CupidConf, CupidSession, CupidUtil}
import com.aliyun.odps.request.cupid.webproxy.PollCallWebProxy
import org.apache.log4j.Logger

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object YarnClientImplUtil {
  val logger = Logger.getLogger(this.getClass().getName())

  def getClusterTuboNum(): Int = {
    // TODO
    // val cupidTaskOperator = CupidTaskOperator.newBuilder()
    // cupidTaskOperator.setMoperator("getclustertubonum")
    // cupidTaskOperator.setMlookupName("")
    // val cupidTaskParamBuilder = CupidTaskParam.newBuilder()
    // cupidTaskParamBuilder.setMcupidtaskoperator(cupidTaskOperator.build())
    // val clusterTuboNumSubmitJob = SubmitJobUtil.getClusterTuboNumSubmitJob(cupidTaskParamBuilder.build())
    // return clusterTuboNumSubmitJob
    return 100
  }

  def getNewApplicationResponse(): GetNewApplicationResponseProto = {
    val getNewApplicationResponseProtoBuilder = GetNewApplicationResponseProto.newBuilder()
    val applicationIdProtoOrBuilder = ApplicationIdProto.newBuilder()
    val appId = getRanDomAppId()
    applicationIdProtoOrBuilder.setId(appId)
    applicationIdProtoOrBuilder.setClusterTimestamp(System.currentTimeMillis())
    getNewApplicationResponseProtoBuilder.setApplicationId(applicationIdProtoOrBuilder.build())
    val resourceBuilder = ResourceProto.newBuilder()
    resourceBuilder.setMemory(96000)
    resourceBuilder.setVirtualCores(128)
    getNewApplicationResponseProtoBuilder.setMaximumCapability(resourceBuilder.build())
    logger.info("appId " + appId + "," + applicationIdProtoOrBuilder.getClusterTimestamp)
    getNewApplicationResponseProtoBuilder.build()
  }

  def getRanDomAppId(): Int = {
    val random = new Random()
    val appId = random.nextInt(Int.MaxValue) % Int.MaxValue + 1
    appId
  }

  def initCupidSession(cupidConfParam: HashMap[String, String]) {
    val conf = new CupidConf()
    cupidConfParam.foreach(cupidConfParamIterm => conf.set(cupidConfParamIterm._1, cupidConfParamIterm._2))
    CupidSession.setConf(conf)
  }

  def pollAMStatus(ins: Instance): CupidTaskDetailResultParam = {
    CupidUtil.getResult(ins)
  }

  def genCupidTrackUrl(amInstance: Instance, appId: String, webUrl: String,
                       overrideCupidSession: CupidSession = null): String = {
    val cupidSession = overrideCupidSession match {
      case null => CupidSession.get
      case _ => overrideCupidSession
    }

    val trackurlHost = cupidSession.conf.get("odps.moye.trackurl.host",
      "http://jobview.odps.aliyun-inc.com")
    val trackUrl = new TrackUrl(cupidSession.odps, trackurlHost)
    val hours = cupidSession.conf.get("odps.moye.trackurl.dutation", "72")
    val cupidType = cupidSession.conf.get("odps.moye.runtime.type", "spark")
    val lookupName = cupidSession.getJobLookupName
    if (trackurlHost != "") {
      trackUrl.setLogViewHost(trackurlHost)
    }
    val webproxyEndPoint = cupidSession.conf.get("odps.cupid.webproxy.endpoint",
      "http://service.odps.aliyun-inc.com/api")
    val trackUrlStr = trackUrl.genCupidTrackUrl(amInstance,
      appId,
      webUrl,
      hours,
      cupidType,
      lookupName,
      webproxyEndPoint)

    if (cupidSession.conf.get("odps.moye.test.callwebproxy", "false") == "true") {
      logger.info("start to call the webproxy")
      val interval = cupidSession.conf.get("odps.moye.test.callwebproxy.interval", "10")
      PollCallWebProxy.getInstance().startPoll(trackUrlStr, interval.toInt)
    }
    trackUrlStr
  }

  def transformAppCtxAndStartAM(paramForFM: HashMap[String, String],
                                odpsLocalResource: OdpsLocalResource,
                                overrideCupidSession: CupidSession = null): Instance = {
    val cupidSession = overrideCupidSession match {
      case null => CupidSession.get
      case _ => overrideCupidSession
    }

    val cupidTaskOperator = CupidTaskParamProtos.CupidTaskOperator.newBuilder()
    cupidTaskOperator.setMoperator("startam")
    cupidTaskOperator.setMlookupName("")
    val engineType = paramForFM.get(SDKConstants.ENGINE_RUNNING_TYPE)
    val jobPriority = Option(paramForFM.get(SDKConstants.CUPID_JOB_PRIORITY)).getOrElse("1").toInt
    if (engineType != null) {
      cupidTaskOperator.setMenginetype(engineType)
    }
    val jobConfBuilder = JobConf.newBuilder()
    val jobConfItemBuilder = JobConfItem.newBuilder()
    val iterator = paramForFM.entrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      if (entry.getKey.startsWith("cupid")
        || entry.getKey.startsWith("odps.moye")
        || entry.getKey.startsWith("odps.executor")
        || entry.getKey.startsWith("odps.cupid")) {
        jobConfItemBuilder.setKey(entry.getKey)
        jobConfItemBuilder.setValue(entry.getValue)
        jobConfBuilder.addJobconfitem(jobConfItemBuilder.build())
      }
    }

    val cupidTaskParamBuilder = CupidTaskParamProtos.CupidTaskParam.newBuilder()
    cupidTaskParamBuilder.setMcupidtaskoperator(cupidTaskOperator.build())
    cupidTaskParamBuilder.setJobconf(jobConfBuilder.build())
    cupidTaskParamBuilder.setLocalresource(setProjectForLocalResource(odpsLocalResource))
    var cupidPlanUseResource = false
    if (paramForFM.containsKey("odps.cupid.engine.running.type") && paramForFM.get("odps.cupid.engine.running.type").equals("longtime")) {
      cupidPlanUseResource = true
    }
    val instance = SubmitJobUtil.submitJob(cupidTaskParamBuilder.build(), CupidTaskRunningMode.eHasFuxiJob, Some(jobPriority), cupidPlanUseResource, cupidSession)
    logger.info("transformAppCtxAndStartAM instance id " + instance.getId)
    cupidSession.setJobLookupName(instance.getId)

    // TODO remove later Remain compatible for Client Mode
    CupidSession.get.setJobLookupName(instance.getId)

    CupidUtil.getResult(instance)
    instance
  }

  def setProjectForLocalResource(odpsLocalResource: OdpsLocalResource,
                                 overrideCupidSession: CupidSession = null): OdpsLocalResource = {

    val cupidSession = overrideCupidSession match {
      case null => CupidSession.get
      case _ => overrideCupidSession
    }

    val odpsLocalResourceBuilder = CupidTaskParamProtos.OdpsLocalResource.newBuilder()
    val odpsLocalResourceBuilderItemBuilder = CupidTaskParamProtos.OdpsLocalResourceItem.newBuilder()

    odpsLocalResource.getLocalresourceitemList.asScala.foreach(OdpsLocalResourceItem => {
      odpsLocalResourceBuilderItemBuilder.setProjectname(cupidSession.conf.get("odps.project.name"))
      odpsLocalResourceBuilderItemBuilder.setRelativefilepath(OdpsLocalResourceItem.getRelativefilepath)
      odpsLocalResourceBuilderItemBuilder.setType(OdpsLocalResourceItem.getType)
      odpsLocalResourceBuilder.addLocalresourceitem(odpsLocalResourceBuilderItemBuilder.build())
    }
    )
    odpsLocalResourceBuilder.build()
  }
}
