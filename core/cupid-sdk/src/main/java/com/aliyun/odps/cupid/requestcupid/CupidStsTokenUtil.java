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

import apsara.odps.cupid.protocol.StsTokenInfoProtos.*;
import com.aliyun.odps.cupid.CupidSession;
import com.aliyun.odps.cupid.runtime.RuntimeContext;

public class CupidStsTokenUtil {

    /**
     * need to send cupid task, so need cupidSession Object
     *
     * @param roleArn
     * @param stsTokenType
     * @param cupidSession
     */
    public static void GenerateStsToken(String roleArn, String stsTokenType, CupidSession cupidSession) throws Exception {
        String instanceId = System.getenv("META_LOOKUP_NAME");
        GenerateStsTokenReq.Builder generateStsTokenReq = GenerateStsTokenReq.newBuilder();
        generateStsTokenReq.setInstanceId(instanceId);
        generateStsTokenReq.setRoleArn(roleArn);
        generateStsTokenReq.setStsTokenType(stsTokenType);

        CupidSetInformationUtil.cupidSetInformation(
                instanceId,
                "RegisterStsToken",
                new String(generateStsTokenReq.build().toByteArray()),
                cupidSession
        );
    }

    public static StsTokenInfo FetchStsToken(String roleArn, String stsTokenType) throws Exception {
        GetStsTokenRes res = RuntimeContext.get().getStsToken(roleArn, stsTokenType);
        return new StsTokenInfo(
                res.getAkId(),
                res.getAkSecret(),
                res.getExpirationTime(),
                res.getToken(),
                stsTokenType
        );
    }

}
