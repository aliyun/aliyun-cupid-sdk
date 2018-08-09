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

package com.aliyun.odps.cupid.util;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.cupid.CupidSession;
import com.aliyun.odps.cupid.requestcupid.CupidProxyTokenUtil;
import com.aliyun.odps.security.SecurityManager;
import org.apache.log4j.Logger;

public class TrackUrl {

    private Odps odps = null;
    private String logViewHost;
    private String POLICY_TYPE = "BEARER";
    private static Logger logger = Logger.getLogger(TrackUrl.class);

    public void setLogViewHost(String logViewHost) {
        this.logViewHost = logViewHost;
    }

    public String getLogViewHost() {
        return this.logViewHost;
    }

    public TrackUrl(Odps odps, String logViewHost) throws OdpsException {
        this.odps = odps;
        this.logViewHost = logViewHost;

        if (odps.getLogViewHost() != null) {
            this.logViewHost = odps.getLogViewHost();
        }
    }

    public TrackUrl(Odps odps) {
        this.odps = odps;
    }

    public String genToken(Instance amInstance, long hours) throws OdpsException {
        SecurityManager sm = odps.projects().get(amInstance.getProject()).getSecurityManager();
        String policy = this.generatePolicy(amInstance, hours);
        return sm.generateAuthorizationToken(policy, POLICY_TYPE);
    }

    public String genCupidTrackUrl(Instance amInstance,
                                   String appId,
                                   String webUrl,
                                   String hours,
                                   String runTimeType,
                                   String metaName,
                                   String webproxyEndPoint) throws OdpsException {
        SecurityManager sm = odps.projects().get(amInstance.getProject()).getSecurityManager();
        String policy = this.generatePolicy(amInstance, Long.parseLong(hours));
        String token = sm.generateAuthorizationToken(policy, POLICY_TYPE);
        String logview = logViewHost + "/proxyview/jobview/?h=" + webproxyEndPoint + "&p=" +
                amInstance.getProject() + "&i=" + amInstance.getId() + "&t=" + runTimeType + "&id=" + appId + "&metaname=" + metaName + "&token=" + token;
        return logview;
    }

    public String genCupidTrackUrl(Instance amInstance,
                                   String appId,
                                   String webUrl,
                                   String hours,
                                   String runTimeType,
                                   String metaName,
                                   String webproxyEndPoint,
                                   CupidSession cupidSession) throws OdpsException {
        SecurityManager sm = odps.projects().get(amInstance.getProject()).getSecurityManager();
        String policy = this.generatePolicy(amInstance, Long.parseLong(hours));
        String token = sm.generateAuthorizationToken(policy, POLICY_TYPE);
        String params = "?h=" + webproxyEndPoint + "&p=" +
                amInstance.getProject() + "&i=" + amInstance.getId() + "&t=" + runTimeType + "&id=" + appId + "&metaname=" + metaName + "&token=" + token;

        String proxyToken = "";
        try {
            proxyToken = CupidProxyTokenUtil.getProxyToken(amInstance.getId(), null, Integer.valueOf(hours), cupidSession);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        String logview = proxyToken + "." + cupidSession.conf.get("odps.cupid.proxy.end.point", "sparkui.cupid.proxy.taobao.org") + params;

        return logview;
    }

    public String generatePolicy(Instance instance, long hours) {
        String policy = "{\n" + "    \"expires_in_hours\": " +
                hours + ",\n" + "    \"policy\": {\n" +
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
