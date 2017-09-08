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

package com.aliyun.odps.cupid.util

import com.aliyun.odps.security.SecurityManager
import com.aliyun.odps.{Instance, Odps}

class TrackUrl(odps: Odps, var logViewHost: String) {

  val POLICY_TYPE: String = "BEARER"

  if (odps.getLogViewHost() != null) {
    this.logViewHost = odps.getLogViewHost()
  }

  def getLogViewHost(): String = {
    this.logViewHost
  }

  def setLogViewHost(logViewHost: String) {
    this.logViewHost = logViewHost
  }

  def genCupidTrackUrl(amInstance: Instance, appId: String, webUrl: String, hours: String, runTimeType: String, metaName: String, webproxyEndPoint: String): String = {
    val sm: SecurityManager = odps.projects().get(amInstance.getProject()).getSecurityManager()
    val policy: String = this.generatePolicy(amInstance, hours.toLong)
    val token: String = sm.generateAuthorizationToken(policy, POLICY_TYPE)
    val logview: String = logViewHost + "/proxyview/jobview/?h=" + webproxyEndPoint + "&p=" +
      amInstance.getProject() + "&i=" + amInstance.getId() + "&t=" + runTimeType + "&id=" + appId + "&metaname=" + metaName + "&token=" + token
    logview
  }

  def generatePolicy(instance: Instance, hours: Long): String = {
    val policy: String = "{\n" + "    \"expires_in_hours\": " +
      hours.toString + ",\n" + "    \"policy\": {\n" +
      "        \"Statement\": [{\n" +
      "            \"Action\": [\"odps:Read\"],\n" +
      "            \"Effect\": \"Allow\",\n" +
      "            \"Resource\": \"acs:odps:*:projects/" +
      instance.getProject() +
      "/instances/" + instance.getId() + "\"\n" +
      "        }],\n" +
      "        \"Version\": \"1\"\n" + "    }\n" + "}";
    return policy;
  }
}
