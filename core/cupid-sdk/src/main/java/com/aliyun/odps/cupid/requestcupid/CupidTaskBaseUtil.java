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
import com.aliyun.odps.cupid.CupidConf;
import com.aliyun.odps.cupid.CupidSession;
import com.aliyun.odps.cupid.utils.JTuple;
import org.apache.log4j.Logger;

public class CupidTaskBaseUtil {

    private static Logger logger = Logger.getLogger(CupidTaskBaseUtil.class);

    /**
     * Pass conf to CupidTask server side
     * @param cupidSession
     * @return
     */
    private static JobConf.Builder getConfBuilder(CupidSession cupidSession)
    {
        CupidConf appConf = cupidSession.conf;
        JobConf.Builder jobConfBuilder = JobConf.newBuilder();
        JobConfItem.Builder jobConfItemBuilder = JobConfItem.newBuilder();

        for (JTuple.JTuple2<String, String> appConfItem : appConf.getAll())
        {
            if (appConfItem._1().startsWith("odps")
                    || appConfItem._1().startsWith("cupid")
                    || appConfItem._1().startsWith("odps.moye")
                    || appConfItem._1().startsWith("odps.executor")
                    || appConfItem._1().startsWith("odps.cupid")) {
                jobConfItemBuilder.setKey(appConfItem._1());
                jobConfItemBuilder.setValue(appConfItem._2());
                jobConfBuilder.addJobconfitem(jobConfItemBuilder.build());
            }
        }
        return jobConfBuilder;
    }

    /**
     * CupidTaskParamBuilder with cupidTaskOperator and conf
     * @param cupidSession
     * @param cupidTaskOperator
     * @return
     */
    public static CupidTaskParam.Builder getOperationBaseInfo(CupidSession cupidSession,
                                                              String cupidTaskOperator)
    {
        return getOperationBaseInfo(cupidSession, cupidTaskOperator, null, null);
    }

    /**
     * CupidTaskParamBuilder with cupidTaskOperator/lookupName/engineType and conf
     * @param cupidSession
     * @param cupidTaskOperator
     * @param lookupName
     * @param engineType
     * @return
     */
    public static CupidTaskParam.Builder getOperationBaseInfo(CupidSession cupidSession,
                                                              String cupidTaskOperator,
                                                              String lookupName,
                                                              String engineType)
    {
        CupidTaskParam.Builder cupidTaskParamBuilder = CupidTaskParam.newBuilder();
        CupidTaskOperator.Builder cupidTaskOperatorBuilder = CupidTaskOperator.newBuilder();

        cupidTaskOperatorBuilder.setMoperator(cupidTaskOperator);
        if (lookupName != null)
        {
            cupidTaskOperatorBuilder.setMlookupName(lookupName);
        }

        if (engineType != null)
        {
            cupidTaskOperatorBuilder.setMenginetype(engineType);
        }

        cupidTaskParamBuilder.setMcupidtaskoperator(cupidTaskOperatorBuilder.build());
        cupidTaskParamBuilder.setJobconf(getConfBuilder(cupidSession).build());
        return cupidTaskParamBuilder;
    }
}
