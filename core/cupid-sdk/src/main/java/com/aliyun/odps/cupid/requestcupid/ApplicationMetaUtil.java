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
import com.aliyun.odps.cupid.CupidException;
import com.aliyun.odps.cupid.CupidSession;
import org.apache.log4j.Logger;

public class ApplicationMetaUtil {

    private static Logger logger = Logger.getLogger(ApplicationMetaUtil.class);

    /**
     * CupidSession thread-safe call
     * @param applicationType
     * @param applicationId
     * @param instanceId
     * @param applicationTags
     * @param applicationName
     * @param cupidSession
     */
    public static void createApplicationMeta(String applicationType,
                                             String applicationId,
                                             String instanceId,
                                             String applicationTags,
                                             String applicationName,
                                             CupidSession cupidSession) throws Exception {
        CupidTaskParam.Builder cupidTaskParamBuilder =
                CupidTaskBaseUtil.getOperationBaseInfo(
                        cupidSession,
                        CupidTaskOperatorConst.CUPID_TASK_CREATE_APPLICATION_META
                );

        CreateApplicationMetaInfo.Builder createApplicationMetaInfo = CreateApplicationMetaInfo.newBuilder();
        createApplicationMetaInfo.setApplicationId(applicationId);
        createApplicationMetaInfo.setApplicationTags(applicationTags);
        createApplicationMetaInfo.setInstanceId(instanceId);
        createApplicationMetaInfo.setApplicationType(applicationType);
        createApplicationMetaInfo.setRunningMode(
                cupidSession.conf.get("odps.cupid.engine.running.type", "default")
        );
        createApplicationMetaInfo.setApplicationName(applicationName);

        cupidTaskParamBuilder.setCreateApplicationMetaInfo(createApplicationMetaInfo.build());
        SubmitJobUtil.createApplicationMetaSubmitJob(cupidTaskParamBuilder.build(), cupidSession);
    }

    /**
     * CupidSession thread-safe call
     * @param applicationId
     * @param cupidSession
     * @return
     */
    public static ApplicationMeta getApplicationMeta(String applicationId,
                                                     CupidSession cupidSession) throws Exception {
        CupidTaskParam.Builder cupidTaskParamBuilder =
                CupidTaskBaseUtil.getOperationBaseInfo(
                        cupidSession,
                        CupidTaskOperatorConst.CUPID_TASK_GET_APPLICATION_META
                );

        GetApplicationMetaInfo.Builder getApplicationMetaInfo = GetApplicationMetaInfo.newBuilder();
        getApplicationMetaInfo.setApplicationId(applicationId);
        cupidTaskParamBuilder.setGetApplicationMetaInfo(getApplicationMetaInfo.build());
        return SubmitJobUtil.getApplicationMetaSubmitJob(cupidTaskParamBuilder.build(), cupidSession);
    }

    /**
     * CupidSession thread-safe call
     * @param instanceId
     * @param cupidSession
     * @return null means not such cupid instance exists
     */
    public static ApplicationMeta getCupidInstanceMeta(String instanceId,
                                                       CupidSession cupidSession) throws Exception {
        CupidTaskParam.Builder cupidTaskParamBuilder =
                CupidTaskBaseUtil.getOperationBaseInfo(
                        cupidSession,
                        CupidTaskOperatorConst.CUPID_TASK_GET_APPLICATION_META
                );

        try
        {
            GetApplicationMetaInfo.Builder getApplicationMetaInfo = GetApplicationMetaInfo.newBuilder();
            getApplicationMetaInfo.setInstanceId(instanceId);
            cupidTaskParamBuilder.setGetApplicationMetaInfo(getApplicationMetaInfo.build());
            return SubmitJobUtil.getApplicationMetaSubmitJob(cupidTaskParamBuilder.build(), cupidSession);
        }
        catch (CupidException ex)
        {
            logger.error("getCupidInstanceMeta via instanceId: " + instanceId + " failed with errMsg: " + ex.getMessage(), ex);
            return null;
        }
    }

    /**
     * CupidSession thread-safe call
     *
     *
     * @param applicationTypes A comma-separated applicationType like spark,hive,other
     * @param yarnApplicationStates A comma-separated yarnApplicationStates like 0,1,7,other
     * @param cupidSession
     * @return
     */
    public static ApplicationMetaList listApplicationMeta(String applicationTypes,
                                                          String yarnApplicationStates,
                                                          CupidSession cupidSession) throws Exception {
        CupidTaskParam.Builder cupidTaskParamBuilder =
                CupidTaskBaseUtil.getOperationBaseInfo(
                        cupidSession,
                        CupidTaskOperatorConst.CUPID_TASK_LIST_APPLICATION_META
                );

        ListApplicationMetaInfo.Builder listApplicationMetaInfo = ListApplicationMetaInfo.newBuilder();
        if (applicationTypes != null && !applicationTypes.equals("")) {
            listApplicationMetaInfo.setApplicationTypes(applicationTypes);
        }

        if (yarnApplicationStates != null && !yarnApplicationStates.equals("")) {
            listApplicationMetaInfo.setYarnApplicationStates(yarnApplicationStates);
        }

        cupidTaskParamBuilder.setListApplicationMetaInfo(listApplicationMetaInfo.build());
        return SubmitJobUtil.listApplicationMetaSubmitJob(cupidTaskParamBuilder.build(), cupidSession);
    }

    /**
     * CupidSession thread-safe call
     * @param applicationId
     * @param cupidSession
     */
    public static void updateApplicationMeta(String applicationId,
                                             ApplicationMeta applicationMeta,
                                             CupidSession cupidSession) throws Exception {
        CupidTaskParam.Builder cupidTaskParamBuilder =
                CupidTaskBaseUtil.getOperationBaseInfo(
                        cupidSession,
                        CupidTaskOperatorConst.CUPID_TASK_UPDATE_APPLICATION_META
                );

        UpdateApplicationMetaInfo.Builder updateApplicationMetaInfo = UpdateApplicationMetaInfo.newBuilder();
        updateApplicationMetaInfo.setApplicationId(applicationId);
        updateApplicationMetaInfo.setApplicationMeta(applicationMeta);

        cupidTaskParamBuilder.setUpdateApplicationMetaInfo(updateApplicationMetaInfo.build());
        SubmitJobUtil.updateApplicationMetaSubmitJob(cupidTaskParamBuilder.build(), cupidSession);
    }
}
