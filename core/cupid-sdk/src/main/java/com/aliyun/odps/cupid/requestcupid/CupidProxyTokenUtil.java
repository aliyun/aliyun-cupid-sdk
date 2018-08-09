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

package com.aliyun.odps.cupid.requestcupid;

import apsara.odps.cupid.protocol.CupidTaskParamProtos.*;
import com.aliyun.odps.cupid.CupidSession;
import org.apache.log4j.Logger;

public class CupidProxyTokenUtil {


    private static Logger logger = Logger.getLogger(CupidProxyTokenUtil.class);

    /**
     * CupidSession thread-safe call
     * @param instanceId
     * @param appName
     * @param expiredInHours
     * @param cupidSession
     * @return
     */
    public static String getProxyToken(String instanceId,
                                       String appName,
                                       int expiredInHours,
                                       CupidSession cupidSession) throws Exception {
        CupidTaskParam.Builder cupidTaskParamBuilder =
                CupidTaskBaseUtil.getOperationBaseInfo(
                        cupidSession,
                        CupidTaskOperatorConst.CUPID_TASK_GET_PROXY_TOKEN
                );

        CupidProxyTokenRequest.Builder cupidProxyTokenRequest = CupidProxyTokenRequest.newBuilder();
        cupidProxyTokenRequest.setInstanceId(instanceId);
        cupidProxyTokenRequest.setAppName(appName);
        cupidProxyTokenRequest.setExpiredInHours(expiredInHours);
        cupidTaskParamBuilder.setCupidProxyTokenRequest(cupidProxyTokenRequest.build());
        return SubmitJobUtil.getProxyTokenSubmitJob(cupidTaskParamBuilder.build(), cupidSession);
    }
}
