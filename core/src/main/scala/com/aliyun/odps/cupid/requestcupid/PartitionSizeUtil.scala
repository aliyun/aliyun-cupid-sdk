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
import com.aliyun.odps.cupid.{CupidConf, CupidSession, CupidUtil}
import org.apache.log4j.Logger

object PartitionSizeUtil {
  val logger = Logger.getLogger(this.getClass().getName())

  def getMultiTablesPartitonSize(multiTablesInputInfos: MultiTablesInputInfos): GetPartitionSizeResult = {
    var cupidTaskParamBuilder = getPartitionSizeBaseInfo
    cupidTaskParamBuilder.setMultiTablesInputInfos(multiTablesInputInfos)
    SubmitJobUtil.getPartitionSizeSubmitJob(cupidTaskParamBuilder.build())
  }

  def getPartitionSizeBaseInfo(): CupidTaskParam.Builder = {
    var moyeTaskParamBuilder = CupidTaskParam.newBuilder()
    var moyeTaskOperatorBuilder = CupidTaskOperator.newBuilder()
    moyeTaskOperatorBuilder.setMoperator(CupidTaskOperatorConst.CUPID_TASK_GETPARTITIONSIZE)
    moyeTaskOperatorBuilder.setMlookupName(CupidUtil.getEngineLookupName())
    val appConf: CupidConf = CupidSession.get.conf
    moyeTaskParamBuilder.setMcupidtaskoperator(moyeTaskOperatorBuilder.build())

    val jobConfBuilder = JobConf.newBuilder()
    var jobConfItemBuilder = JobConfItem.newBuilder()

    val appConfAll = appConf.getAll
    for (appConfItem <- appConfAll) {
      appConfItem._1 match {
        case "odps.input.split.size" | "odps.moye.max.table.partition.num" |
             "odps.moye.max.table.num" | "odps.moye.max.split.num" |
             "odps.cupid.hive.datatype.compatible" => {
          jobConfItemBuilder.setKey(appConfItem._1)
          jobConfItemBuilder.setValue(appConfItem._2)
          jobConfBuilder.addJobconfitem(jobConfItemBuilder.build())
        }
        case _ =>
      }
    }
    moyeTaskParamBuilder.setJobconf(jobConfBuilder.build())
    moyeTaskParamBuilder
  }

  def getPartitonSize(project: String, table: String, columns: Array[String], partition: Array[String], splitSize: Int, splitCount: Int, odpsRddId: Int = 0, splitTempDir: String = ""): GetPartitionSizeResult = {
    var multiTablesInputInfosBuilder = MultiTablesInputInfos.newBuilder()
    multiTablesInputInfosBuilder.setSplitCount(splitCount)
    multiTablesInputInfosBuilder.setSplitSize(splitSize)
    multiTablesInputInfosBuilder.setInputId(odpsRddId)
    if (splitTempDir != "") multiTablesInputInfosBuilder.setSplitTempDir(splitTempDir)
    var multiTablesInputInfoItemBuilder = MultiTablesInputInfoItem.newBuilder()
    multiTablesInputInfoItemBuilder.setProjName(project)
    multiTablesInputInfoItemBuilder.setTblName(table)
    var columnsStr = ""
    if (columns.length > 0) {
      columnsStr = columns(0)
      for (i <- 1 until columns.length) columnsStr += "," + columns(i)
    }
    if (columnsStr != "")
      logger.info("The user columnStr = " + columnsStr)
    multiTablesInputInfoItemBuilder.setCols(columnsStr)
    if (partition.size > 0) {
      for (partSpecTmp <- partition) {
        multiTablesInputInfoItemBuilder.setPartSpecs(partSpecTmp)
        multiTablesInputInfosBuilder.addMultiTablesInputInfoItem(multiTablesInputInfoItemBuilder.build())
      }
    } else {
      multiTablesInputInfosBuilder.addMultiTablesInputInfoItem(multiTablesInputInfoItemBuilder.build())
    }

    var cupidTaskParamBuilder = getPartitionSizeBaseInfo
    cupidTaskParamBuilder.setMultiTablesInputInfos(multiTablesInputInfosBuilder.build())
    SubmitJobUtil.getPartitionSizeSubmitJob(cupidTaskParamBuilder.build())
  }
}