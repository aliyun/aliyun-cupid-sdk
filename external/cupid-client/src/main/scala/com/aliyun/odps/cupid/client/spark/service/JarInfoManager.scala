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

package com.aliyun.odps.cupid.client.spark.service

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import com.aliyun.odps.cupid.client.spark.service.JarInfoManager.JarInfo

object JarInfoManager {

  case class JarInfo(jarName: String, jarLocalPath: String, jarPanguPath: String) {
    override def toString: String = {
      "jarName=" + jarName + ",jarLocalPath=" + jarLocalPath + ",jarPanguPath=" + jarPanguPath
    }
  }

}

class JarInfoManager {
  private val jarInfos: ConcurrentMap[String, JarInfo] = new ConcurrentHashMap[String, JarInfo]

  def getJarInfo(jarName: String): JarInfo = {
    jarInfos.get(jarName)
  }

  def addJarInfo(jarName: String, jarInfo: JarInfo) = {
    jarInfos.put(jarName, jarInfo)
  }
}
