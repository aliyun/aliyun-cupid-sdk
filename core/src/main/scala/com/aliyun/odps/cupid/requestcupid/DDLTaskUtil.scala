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
import com.aliyun.odps.PartitionSpec
import com.aliyun.odps.cupid.{CupidSession, CupidUtil}
import org.apache.log4j.Logger

import scala.collection.mutable.{ArrayBuffer, Set}

object DDLTaskUtil {
  val logger = Logger.getLogger(this.getClass().getName())

  def doDDLTaskForSaveMultiParittion(project: String, table: String, isOverWrite: Boolean, ddlTaskPaths: Set[String]) {
    val ddlInfoBuilder = getDDLTaskBaseInfo(project, table, isOverWrite)
    addDDLInfoItermForSaveMultiParittion(ddlInfoBuilder, ddlTaskPaths)
    val moyeTaskParamBuilder = getMoyeTaskParamInfo
    moyeTaskParamBuilder.setDdlInfo(ddlInfoBuilder.build())
    SubmitJobUtil.ddlTaskSubmitJob(moyeTaskParamBuilder.build())
  }

  def getMoyeTaskParamInfo(): CupidTaskParam.Builder = {
    val moyeTaskParamBuilder = CupidTaskParam.newBuilder()
    val moyeTaskOperatorBuilder = CupidTaskOperator.newBuilder()
    moyeTaskOperatorBuilder.setMoperator(CupidTaskOperatorConst.CUPID_TASK_DDLTASK)
    moyeTaskOperatorBuilder.setMlookupName(CupidUtil.getEngineLookupName())

    //setup jobConf & appConf
    val jobConfBuilder = JobConf.newBuilder()
    var jobConfItemBuilder = JobConfItem.newBuilder()
    val appConfAll = CupidSession.get.conf.getAll
    for (appConfItem <- appConfAll) {
      jobConfItemBuilder.setKey(appConfItem._1)
      jobConfItemBuilder.setValue(appConfItem._2)
      jobConfBuilder.addJobconfitem(jobConfItemBuilder.build())
    }
    moyeTaskParamBuilder.setJobconf(jobConfBuilder.build())
    moyeTaskParamBuilder.setMcupidtaskoperator(moyeTaskOperatorBuilder.build())
  }

  def getDDLTaskBaseInfo(project: String, table: String, isOverWrite: Boolean): DDLInfo.Builder = {
    val ddlInfoBuilder = DDLInfo.newBuilder()
    ddlInfoBuilder.setSaveTableProject(project)
    ddlInfoBuilder.setSaveTableName(table)
    ddlInfoBuilder.setIsOverWrite(isOverWrite)
    ddlInfoBuilder
  }

  def addDDLInfoItermForSaveMultiParittion(ddlInfoBuilder: DDLInfo.Builder, ddlTaskPaths: Set[String]) {
    val ddlTaskPathsArray = ddlTaskPaths.toArray
    val ddlInfoItermBuilder = DDLInfoIterm.newBuilder()
    var outPutPartitionInfo: ArrayBuffer[String] = ArrayBuffer[String]()
    for (ddlTaskPath <- ddlTaskPathsArray) {
      val pathParams = ddlTaskPath.split("/");
      val pathPectOut = ParsePartSpec(pathParams(pathParams.length - 1))
      ddlInfoItermBuilder.setPanguTempDirPath(ddlTaskPath)
      ddlInfoItermBuilder.setPartSpec(pathPectOut)
      if (outPutPartitionInfo.size < 3) {
        outPutPartitionInfo += pathPectOut.toString()
      }
      ddlInfoBuilder.addDdlInfoIterms(ddlInfoItermBuilder.build())
    }
    var outputPartitionMesg = "the saved partition num = " + ddlTaskPathsArray.size + ",show some partition" + "\n"
    outPutPartitionInfo.foreach(f => outputPartitionMesg = outputPartitionMesg + f + "\n")
    logger.info(outputPartitionMesg)
  }

  def ParsePartSpec(partSpecIn: String): String = {
    val parsePartSpec = new PartitionSpec(partSpecIn)
    val parsePartOut = parsePartSpec.toString().trim().replaceAll("'", "")
    parsePartOut
  }

  def doDDLTaskForSaveMultiTables(ddlMultiTables: DDLMultiTableInfos) {
    val moyeTaskParamBuilder = getMoyeTaskParamInfo
    moyeTaskParamBuilder.setDdlMultiTableInfos(ddlMultiTables)
    SubmitJobUtil.ddlTaskSubmitJob(moyeTaskParamBuilder.build())
  }

  def doDDLTaskForNoarmalSave(project: String, table: String, partSpecOut: String, isOverWrite: Boolean, panguTempDirPath: String) {
    val ddlInfoBuilder = getDDLTaskBaseInfo(project, table, isOverWrite)
    val ddlInfoItermBuilder = DDLInfoIterm.newBuilder()
    ddlInfoItermBuilder.setPanguTempDirPath(panguTempDirPath)
    ddlInfoItermBuilder.setPartSpec(partSpecOut)
    ddlInfoBuilder.addDdlInfoIterms(ddlInfoItermBuilder.build())

    val moyeTaskParamBuilder = getMoyeTaskParamInfo

    moyeTaskParamBuilder.setDdlInfo(ddlInfoBuilder.build())
    SubmitJobUtil.ddlTaskSubmitJob(moyeTaskParamBuilder.build())
  }
}