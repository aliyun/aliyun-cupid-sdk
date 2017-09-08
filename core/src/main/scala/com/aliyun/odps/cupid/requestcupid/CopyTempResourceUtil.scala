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

import java.io.{File, FileInputStream}

import apsara.odps.cupid.protocol.CupidTaskParamProtos
import apsara.odps.cupid.protocol.CupidTaskParamProtos._
import com.aliyun.odps.cupid.{CupidSession, CupidUtil}
import com.aliyun.odps.{FileResource, Odps}
import org.apache.log4j.Logger

import scala.collection.mutable.ArrayBuffer

object CopyTempResourceUtil {

  val logger = Logger.getLogger(this.getClass().getName())

  /*
   * add file to temp resource and copy it from control cluster to computing cluster
   * @param srcPath local file path
   * @return the relative path to the fuxi job temp dir.
   */
  def addAndCopyTempResource(srcPath: String, odps: Odps): (String, String) = {
    val file = new File(srcPath)
    val in0 = new FileInputStream(file)
    val md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(in0);
    in0.close()
    val tmpResName: String = md5 + "_" + file.getName
    addTempResource(tmpResName, file.getAbsolutePath, odps)
    val resources = Array[String](tmpResName)
    (tmpResName, copyTempResourceToFuxiJobDir(resources).apply(0))
  }

  private def copyTempResourceToFuxiJobDir(tmpResources: Array[String]): Array[String] = {
    val moyeTaskParamBuilder = CupidTaskParam.newBuilder()
    val moyeTaskOperatorBuilder = CupidTaskOperator.newBuilder()
    moyeTaskOperatorBuilder.setMoperator(CupidTaskOperatorConst.CUPID_TASK_COPY_TEMPRESOURCE)
    moyeTaskOperatorBuilder.setMlookupName(CupidUtil.getEngineLookupName())
    moyeTaskParamBuilder.setMcupidtaskoperator(moyeTaskOperatorBuilder.build())

    val odpsLocalResourceBuilder = CupidTaskParamProtos.OdpsLocalResource.newBuilder()
    val odpsLocalResourceBuilderItemBuilder = CupidTaskParamProtos.OdpsLocalResourceItem.newBuilder()
    val ret = new ArrayBuffer[String]()
    tmpResources.foreach(res => {
      ret.append(res)
      odpsLocalResourceBuilderItemBuilder.setProjectname(CupidSession.get.conf.get("odps.project.name"))
      odpsLocalResourceBuilderItemBuilder.setRelativefilepath(res)
      odpsLocalResourceBuilderItemBuilder.setType(CupidTaskParamProtos.LocalResourceType.TempResource)
      odpsLocalResourceBuilder.addLocalresourceitem(odpsLocalResourceBuilderItemBuilder.build())
    })
    moyeTaskParamBuilder.setLocalresource(odpsLocalResourceBuilder.build())

    SubmitJobUtil.copyTempResourceSubmitJob(moyeTaskParamBuilder.build())
    ret.toArray
  }

  def addTempResource(tmpResName: String, file: String, odps: Odps): Unit = {
    val res: FileResource = new FileResource();
    res.setName(tmpResName);
    res.setIsTempResource(true);
    logger.info(s"begin creating tempResource: $tmpResName")
    val conf = CupidSession.getConf
    var retryCount = conf.get("spark.copyFileToRemote.retry.count", "3").toInt
    while (retryCount > 0) {
      val in = new FileInputStream(file)
      try {
        if (odps.resources().exists(odps.getDefaultProject(), tmpResName))
          logger.info(s"tempResource existed: $tmpResName")
        else {
          odps.resources().create(odps.getDefaultProject(), res, in);
          logger.info(s"create tempResource $tmpResName success")
        }
        retryCount = 0;
      } catch {
        case e: Throwable => {
          logger.warn(s"create tempResource $tmpResName fail", e)
          retryCount -= 1;
          if (retryCount == 0) {
            throw e
          }
        }
      } finally {
        in.close()
      }
    }
  }
}
