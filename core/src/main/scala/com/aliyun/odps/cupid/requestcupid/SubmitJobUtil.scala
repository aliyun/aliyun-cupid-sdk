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

import java.io.{ByteArrayInputStream, FileInputStream, InputStreamReader}
import java.util.UUID

import apsara.odps.cupid.protocol.CupidTaskParamProtos.{ApplicationMeta, ApplicationMetaList, CupidTaskParam, GetPartitionSizeResult}
import com.aliyun.odps.cupid.{CupidSession, CupidUtil}
import com.aliyun.odps.task.CupidTask
import com.aliyun.odps.{FileResource, Instance}
import com.google.gson.GsonBuilder
import org.apache.commons.codec.binary.Base64
import org.apache.log4j.Logger

import scala.collection.JavaConversions._

object SubmitJobUtil {
  val logger = Logger.getLogger(this.getClass().getName())

  private[requestcupid] def getPartitionSizeSubmitJob(cupidTaskParamPB: CupidTaskParam): GetPartitionSizeResult = {
    val partitionSizeInstance = submitJob(cupidTaskParamPB, CupidTaskRunningMode.eAsyncNotFuxiJob)
    logger.info("partitionSizeInstance id = " + partitionSizeInstance.getId)
    val getPartitionSizeResultStr = CupidUtil.pollSuccessResult(partitionSizeInstance, "getting partition size")
    GetPartitionSizeResult.parseFrom(Base64.decodeBase64(getPartitionSizeResultStr))
  }

  def submitJob(cupidTaskParamPB: CupidTaskParam, runningMode: String): Instance = {
    submitJob(cupidTaskParamPB, runningMode, None)
  }

  private[requestcupid] def genVolumePanguPathSubmitJob(cupidTaskParamPB: CupidTaskParam): String = {
    val genVolumePanguPathInstance = submitJob(cupidTaskParamPB, CupidTaskRunningMode.eAsyncNotFuxiJob)
    logger.info("genVolumePanguPathInfoInstance id = " + genVolumePanguPathInstance.getId)
    val genVolumePanguPathResult = CupidUtil.pollSuccessResult(genVolumePanguPathInstance, "genVolumePanguPath size")
    genVolumePanguPathResult
  }

  private[requestcupid] def ddlTaskSubmitJob(moyeTaskParamPB: CupidTaskParam) {
    val ddlTaskInstance = submitJob(moyeTaskParamPB, CupidTaskRunningMode.eAsyncNotFuxiJob)
    logger.info("ddlTaskSubmitJob instanceid = " + ddlTaskInstance.getId)
    CupidUtil.pollSuccessResult(ddlTaskInstance, "do ddltask")
  }

  private[requestcupid] def copyTempResourceSubmitJob(moyeTaskParamPB: CupidTaskParam) {
    val cpInstance = submitJob(moyeTaskParamPB, CupidTaskRunningMode.eAsyncNotFuxiJob)
    logger.info("copy temp resource instanceid = " + cpInstance.getId)
    CupidUtil.pollSuccessResult(cpInstance, "copy temp resource")
  }

  /**
    * Get ApplicationMeta, thread-safe CupidSession Call
    *
    * @param moyeTaskParamPB
    * @param cupidSession
    * @return
    */
  private[requestcupid] def getApplicationMetaSubmitJob(moyeTaskParamPB: CupidTaskParam,
                                                        cupidSession: CupidSession): ApplicationMeta = {
    val cpInstance = submitJob(moyeTaskParamPB, CupidTaskRunningMode.eAsyncNotFuxiJob, cupidSession)
    logger.debug("getApplicationMeta instanceid = " + cpInstance.getId)
    val getApplicationMetaResultStr = CupidUtil.pollSuccessResult(cpInstance, "getApplicationMeta")
    ApplicationMeta.parseFrom(Base64.decodeBase64(getApplicationMetaResultStr.getBytes()))
  }

  /**
    * Create ApplicationMeta, thread-safe CupidSession Call
    *
    * @param moyeTaskParamPB
    * @param cupidSession
    */
  private[requestcupid] def createApplicationMetaSubmitJob(moyeTaskParamPB: CupidTaskParam,
                                                           cupidSession: CupidSession): Unit = {
    val cpInstance = submitJob(moyeTaskParamPB, CupidTaskRunningMode.eAsyncNotFuxiJob, cupidSession)
    logger.debug("createApplicationMeta instanceid = " + cpInstance.getId)
    CupidUtil.pollSuccessResult(cpInstance, "createApplicationMeta")
  }

  /**
    * List ApplicationMeta, thread-safe CupidSession Call
    *
    * @param moyeTaskParamPB
    * @param cupidSession
    * @return
    */
  private[requestcupid] def listApplicationMetaSubmitJob(moyeTaskParamPB: CupidTaskParam,
                                                         cupidSession: CupidSession): ApplicationMetaList = {
    val cpInstance = submitJob(moyeTaskParamPB, CupidTaskRunningMode.eAsyncNotFuxiJob, cupidSession)
    logger.debug("listApplicationMeta instanceid = " + cpInstance.getId)
    val listApplicationMetaResultStr = CupidUtil.pollSuccessResult(cpInstance, "listApplicationMeta")
    ApplicationMetaList.parseFrom(Base64.decodeBase64(listApplicationMetaResultStr))
  }

  /**
    * Update ApplicationMeta, thread-safe CupidSession Call
    *
    * @param moyeTaskParamPB
    * @param cupidSession
    */
  private[requestcupid] def updateApplicationMetaSubmitJob(moyeTaskParamPB: CupidTaskParam,
                                                           cupidSession: CupidSession): Unit = {
    val cpInstance = submitJob(moyeTaskParamPB, CupidTaskRunningMode.eAsyncNotFuxiJob, cupidSession)
    logger.debug("updateApplicationMeta instanceid = " + cpInstance.getId)
    CupidUtil.pollSuccessResult(cpInstance, "updateApplicationMeta")
  }

  /**
    * CupidSession thread-safe call
    *
    * @param cupidTaskParamPB
    * @param runningMode
    * @param cupidSession
    * @return
    */
  def submitJob(cupidTaskParamPB: CupidTaskParam, runningMode: String, cupidSession: CupidSession): Instance = {
    submitJob(cupidTaskParamPB, runningMode, None, false, cupidSession)
  }

  /**
    * CupidSession thread-safe call
    *
    * @param cupidTaskParamPB
    * @param runningMode
    * @param priority
    * @param planUseResource
    * @param cupidSession
    * @return
    */
  def submitJob(cupidTaskParamPB: CupidTaskParam,
                runningMode: String,
                priority: Option[Int],
                planUseResource: Boolean = false,
                cupidSession: CupidSession = null): Instance = {
    val submitCupidSession = cupidSession match {
      case null => CupidSession.get
      case _ => cupidSession
    }

    val cupidTaskInfo = RetryUtil.retryFunction(
      () => {
        uploadRes(cupidTaskParamPB, planUseResource, submitCupidSession)
      },
      RetryUtil.RetryConst.UPLOAD_RESOURCE, 2) + "," + submitCupidSession.odps.getDefaultProject() + "," + runningMode

    val propMap: java.util.HashMap[String, String] = new java.util.HashMap[String, String]
    propMap.put("biz_id", submitCupidSession.biz_id)
    if (submitCupidSession.flightingMajorVersion != null)
      propMap.put("odps.task.major.version", submitCupidSession.flightingMajorVersion)

    // if current submitCupidSession.odps.getAccount is BearerTokenAccount, refresh bearerToken before task submit
    logger.info(s"submitting CupidTask with ${submitCupidSession.odps.getAccount.getType} type, refreshOdps if needed")
    submitCupidSession.refreshOdps

    if (System.getProperty("odps.exec.context.file") != null) {
      val path = System.getProperty("odps.exec.context.file")
      val reader = new InputStreamReader(new FileInputStream(path))
      val gson = new GsonBuilder().create()
      val map = gson.fromJson(reader, classOf[java.util.HashMap[Object, Object]])
      val smap = map.get("settings").asInstanceOf[java.util.Map[String, String]]
      smap.keySet().foreach {
        case k => {
          val v = smap.get(k)
          propMap.put(k, v)
        }
      }
    }

    priority match {
      case None =>
        CupidTask.run(submitCupidSession.odps, submitCupidSession.odps.getDefaultProject(), cupidTaskInfo, propMap)
      case Some(pr) =>
        CupidTask.run(submitCupidSession.odps, submitCupidSession.odps.getDefaultProject(), cupidTaskInfo, propMap, pr)
    }
  }

  /**
    * CupidSession.get Remain compatible
    * TODO. Remove CupidSession.get
    *
    * @param cupidTaskParamPB
    * @param planUseResource
    * @param cupidSession
    * @return
    */
  private def uploadRes(cupidTaskParamPB: CupidTaskParam,
                        planUseResource: Boolean = false,
                        cupidSession: CupidSession = null): String = {
    val submitCupidSession = cupidSession match {
      case null => CupidSession.get
      case _ => cupidSession
    }
    val tmpResName: String = "cupid_plan_" + UUID.randomUUID().toString()
    val res: FileResource = new FileResource();
    res.setName(tmpResName)
    if (!planUseResource) {
      res.setIsTempResource(true)
    } else {
      logger.info("as longtime job,cupid plan use resource " + tmpResName)
    }
    val odps = submitCupidSession.odps
    if (odps.resources().exists(odps.getDefaultProject(), tmpResName))
      odps.resources().update(odps.getDefaultProject(), res, new ByteArrayInputStream(cupidTaskParamPB.toByteArray()))
    else
      odps.resources().create(odps.getDefaultProject(), res, new ByteArrayInputStream(cupidTaskParamPB.toByteArray()))
    tmpResName
  }

  /**
    * Fetch ProxyToken, thread-safe CupidSession Call
    *
    * @param moyeTaskParamPB
    * @param cupidSession
    */
  private[requestcupid] def getProxyTokenSubmitJob(moyeTaskParamPB: CupidTaskParam,
                                                   cupidSession: CupidSession): String = {
    val cpInstance = submitJob(moyeTaskParamPB, CupidTaskRunningMode.eAsyncNotFuxiJob, cupidSession)
    logger.debug("getProxyTokenSubmitJob instanceid = " + cpInstance.getId)
    CupidUtil.pollSuccessResult(cpInstance, "getCupidProxyToken")
  }

  /**
    * Cupid SetInformation, thread-safe CupidSession Call
    *
    * @param moyeTaskParamPB
    * @param cupidSession
    */
  private[requestcupid] def cupidSetInformationSubmitJob(moyeTaskParamPB: CupidTaskParam,
                                                         cupidSession: CupidSession): Unit = {
    val cpInstance = submitJob(moyeTaskParamPB, CupidTaskRunningMode.eAsyncNotFuxiJob, cupidSession)
    logger.debug("cupidSetInformation instanceid = " + cpInstance.getId)
    CupidUtil.pollSuccessResult(cpInstance, "cupidSetInformation")
  }
}
