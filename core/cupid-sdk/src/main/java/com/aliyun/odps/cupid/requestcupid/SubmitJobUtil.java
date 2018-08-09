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

import apsara.odps.cupid.protocol.CupidTaskParamProtos.ApplicationMeta;
import apsara.odps.cupid.protocol.CupidTaskParamProtos.ApplicationMetaList;
import apsara.odps.cupid.protocol.CupidTaskParamProtos.CupidTaskParam;
import apsara.odps.cupid.protocol.CupidTaskParamProtos.GetPartitionSizeResult;
import com.aliyun.odps.FileResource;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.cupid.CupidSession;
import com.aliyun.odps.cupid.CupidUtil;
import com.aliyun.odps.task.CupidTask;
import com.github.rholder.retry.RetryException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class SubmitJobUtil {

    private static Logger logger = Logger.getLogger(SubmitJobUtil.class);

    // TODO
    // private static String uploadRes(CupidTaskParam cupidTaskParamPB, boolean planUseResource) throws OdpsException {
    //     return uploadRes(cupidTaskParamPB, planUseResource, null);
    // }

    /**
     * CupidSession.get Remain compatible
     * TODO. Remove CupidSession.get
     * @param cupidTaskParamPB
     * @param planUseResource
     * @param cupidSession
     * @return
     */
    private static String uploadRes(CupidTaskParam cupidTaskParamPB,
                          boolean planUseResource,
                          CupidSession cupidSession) throws OdpsException {

        CupidSession submitCupidSession = cupidSession == null ? CupidSession.get() : cupidSession;

        String tmpResName = "cupid_plan_" + UUID.randomUUID().toString();
        FileResource res = new FileResource();
        res.setName(tmpResName);
        if(!planUseResource) {
            res.setIsTempResource(true);
        } else {
            logger.info("as longtime job,cupid plan use resource " +tmpResName);
        }
        Odps odps = submitCupidSession.odps;
        if (odps.resources().exists(odps.getDefaultProject(), tmpResName)) {
            odps.resources().update(odps.getDefaultProject(), res, new ByteArrayInputStream(cupidTaskParamPB.toByteArray()));
        }
        else {
            odps.resources().create(odps.getDefaultProject(), res, new ByteArrayInputStream(cupidTaskParamPB.toByteArray()));
        }
        return tmpResName;
    }

    protected static GetPartitionSizeResult getPartitionSizeSubmitJob(CupidTaskParam cupidTaskParamPB)
            throws Exception
    {
        Instance partitionSizeInstance = submitJob(cupidTaskParamPB, CupidTaskRunningMode.eAsyncNotFuxiJob);
        logger.info("partitionSizeInstance id = " + partitionSizeInstance.getId());
        String getPartitionSizeResultStr =
                CupidUtil.pollSuccessResult(partitionSizeInstance, "getting partition size");
        return GetPartitionSizeResult.parseFrom(Base64.decodeBase64(getPartitionSizeResultStr));
    }

    protected static String genVolumePanguPathSubmitJob(CupidTaskParam cupidTaskParamPB)
            throws Exception
    {
        Instance genVolumePanguPathInstance = submitJob(cupidTaskParamPB, CupidTaskRunningMode.eAsyncNotFuxiJob);
        logger.info("genVolumePanguPathInfoInstance id = " + genVolumePanguPathInstance.getId());
        String genVolumePanguPathResult = CupidUtil.pollSuccessResult(genVolumePanguPathInstance, "genVolumePanguPath size");
        return genVolumePanguPathResult;
    }

    protected static void ddlTaskSubmitJob(CupidTaskParam moyeTaskParamPB) throws Exception {
        Instance ddlTaskInstance = submitJob(moyeTaskParamPB, CupidTaskRunningMode.eAsyncNotFuxiJob);
        logger.info("ddlTaskSubmitJob instanceid = " + ddlTaskInstance.getId());
        CupidUtil.pollSuccessResult(ddlTaskInstance, "do ddltask");
    }

    protected static void copyTempResourceSubmitJob(CupidSession session, CupidTaskParam moyeTaskParamPB)
            throws Exception {
        Instance cpInstance = submitJob(moyeTaskParamPB, CupidTaskRunningMode.eAsyncNotFuxiJob, null, false, session);
        logger.info("copy temp resource instanceid = " + cpInstance.getId());
        CupidUtil.pollSuccessResult(cpInstance, "copy temp resource");
    }

    protected static void upgradeApplicationSubmitJob(CupidSession session, CupidTaskParam moyeTaskParamPB) throws Exception {
        Instance instance = submitJob(moyeTaskParamPB, CupidTaskRunningMode.eAsyncNotFuxiJob, null, false, session);
        logger.info("upgrade application instanceid = " + instance.getId());
        CupidUtil.pollSuccessResult(instance, "upgrade application");
    }

    /**
     * Get ApplicationMeta, thread-safe CupidSession Call
     * @param moyeTaskParamPB
     * @param cupidSession
     * @return
     */
    protected static ApplicationMeta getApplicationMetaSubmitJob(
            CupidTaskParam moyeTaskParamPB,
            CupidSession cupidSession) throws  Exception {
        Instance cpInstance = submitJob(moyeTaskParamPB, CupidTaskRunningMode.eAsyncNotFuxiJob, cupidSession);
        logger.debug("getApplicationMeta instanceid = " + cpInstance.getId());
        String getApplicationMetaResultStr = CupidUtil.pollSuccessResult(cpInstance, "getApplicationMeta");
        return ApplicationMeta.parseFrom(Base64.decodeBase64(getApplicationMetaResultStr.getBytes()));
    }

    /**
     * Create ApplicationMeta, thread-safe CupidSession Call
     * @param moyeTaskParamPB
     * @param cupidSession
     */
    protected static void createApplicationMetaSubmitJob(CupidTaskParam moyeTaskParamPB,
                                                         CupidSession cupidSession) throws Exception
    {
        Instance cpInstance = submitJob(moyeTaskParamPB, CupidTaskRunningMode.eAsyncNotFuxiJob, cupidSession);
        logger.debug("createApplicationMeta instanceid = " + cpInstance.getId());
        CupidUtil.pollSuccessResult(cpInstance, "createApplicationMeta");
    }

    /**
     * List ApplicationMeta, thread-safe CupidSession Call
     * @param moyeTaskParamPB
     * @param cupidSession
     * @return
     */
    protected static ApplicationMetaList listApplicationMetaSubmitJob(
            CupidTaskParam moyeTaskParamPB,
            CupidSession cupidSession) throws Exception
    {
        Instance cpInstance = submitJob(moyeTaskParamPB, CupidTaskRunningMode.eAsyncNotFuxiJob, cupidSession);
        logger.debug("listApplicationMeta instanceid = " + cpInstance.getId());
        String listApplicationMetaResultStr = CupidUtil.pollSuccessResult(cpInstance, "listApplicationMeta");
        return ApplicationMetaList.parseFrom(Base64.decodeBase64(listApplicationMetaResultStr));
    }

    /**
     * Update ApplicationMeta, thread-safe CupidSession Call
     * @param moyeTaskParamPB
     * @param cupidSession
     */
    protected static void updateApplicationMetaSubmitJob(CupidTaskParam moyeTaskParamPB,
                                                         CupidSession cupidSession) throws Exception
    {
        Instance cpInstance = submitJob(moyeTaskParamPB, CupidTaskRunningMode.eAsyncNotFuxiJob, cupidSession);
        logger.debug("updateApplicationMeta instanceid = " + cpInstance.getId());
        CupidUtil.pollSuccessResult(cpInstance, "updateApplicationMeta");
    }

    /**
     * Fetch ProxyToken, thread-safe CupidSession Call
     * @param moyeTaskParamPB
     * @param cupidSession
     */
    protected static String getProxyTokenSubmitJob(CupidTaskParam moyeTaskParamPB,
                                                   CupidSession cupidSession) throws Exception
    {
        Instance cpInstance = submitJob(moyeTaskParamPB, CupidTaskRunningMode.eAsyncNotFuxiJob, cupidSession);
        logger.debug("getProxyTokenSubmitJob instanceid = " + cpInstance.getId());
        return CupidUtil.pollSuccessResult(cpInstance, "getCupidProxyToken");
    }

    /**
     * Cupid SetInformation, thread-safe CupidSession Call
     * @param moyeTaskParamPB
     * @param cupidSession
     */
    protected static void cupidSetInformationSubmitJob(CupidTaskParam moyeTaskParamPB,
                                                       CupidSession cupidSession) throws Exception
    {
        Instance cpInstance = submitJob(moyeTaskParamPB, CupidTaskRunningMode.eAsyncNotFuxiJob, cupidSession);
        logger.debug("cupidSetInformation instanceid = " + cpInstance.getId());
        CupidUtil.pollSuccessResult(cpInstance, "cupidSetInformation");
    }


    /**
     * send TaskServiceRequest, thread-safe CupidSession Call
     * @param moyeTaskParamPB
     * @param cupidSession
     */
    protected static String sendTaskServiceRequestSubmitJob(CupidTaskParam moyeTaskParamPB,
                                                            CupidSession cupidSession) throws Exception {
        Instance cpInstance = submitJob(moyeTaskParamPB, CupidTaskRunningMode.eAsyncNotFuxiJob, cupidSession);
        logger.debug("sendTaskServiceRequestSubmitJob instanceid = " + cpInstance.getId());
        return CupidUtil.pollSuccessResult(cpInstance, "sendTaskServiceRequest");
    }

    /**
     * CupidSession thread-safe call
     * @param cupidTaskParamPB
     * @param runningMode
     * @return
     * @throws ExecutionException
     * @throws RetryException
     * @throws OdpsException
     * @throws FileNotFoundException
     */
    public static Instance submitJob(
            CupidTaskParam cupidTaskParamPB,
            String runningMode)
            throws ExecutionException, RetryException, OdpsException, FileNotFoundException {
        return submitJob(cupidTaskParamPB, runningMode, null, false, null);
    }

    /**
     * @param cupidTaskParamPB
     * @param runningMode
     * @param priority
     * @return
     * @throws ExecutionException
     * @throws RetryException
     * @throws OdpsException
     * @throws FileNotFoundException
     */
    public static Instance submitJob(
            CupidTaskParam cupidTaskParamPB,
            String runningMode,
            Integer priority)
            throws ExecutionException, RetryException, OdpsException, FileNotFoundException {
        return submitJob(cupidTaskParamPB, runningMode, priority, false, null);
    }

    /**
     * CupidSession thread-safe call
     * @param cupidTaskParamPB
     * @param runningMode
     * @param cupidSession
     * @return
     * @throws ExecutionException
     * @throws RetryException
     * @throws OdpsException
     * @throws FileNotFoundException
     */
    public static Instance submitJob(CupidTaskParam cupidTaskParamPB,
                                     String runningMode,
                                     CupidSession cupidSession)
            throws ExecutionException, RetryException, OdpsException, FileNotFoundException {
        return submitJob(cupidTaskParamPB, runningMode, null, false, cupidSession);
    }

    /**
     * CupidSession thread-safe call
     * @param cupidTaskParamPB
     * @param runningMode
     * @param priority
     * @param planUseResource
     * @param cupidSession
     * @return
     * @throws ExecutionException
     * @throws RetryException
     * @throws FileNotFoundException
     * @throws OdpsException
     */
    public static Instance submitJob(final CupidTaskParam cupidTaskParamPB,
                                     String runningMode,
                                     Integer priority,
                                     final Boolean planUseResource,
                                     CupidSession cupidSession) throws ExecutionException, RetryException, FileNotFoundException, OdpsException {

        final CupidSession submitCupidSession = cupidSession == null ? CupidSession.get() : cupidSession;

        // if current submitCupidSession.odps.getAccount is BearerTokenAccount, refresh bearerToken before task submit
        logger.info("submitting CupidTask with " + submitCupidSession.odps.getAccount().getType() + " type, refreshOdps if needed");
        submitCupidSession.refreshOdps();

        Callable<String> uploadResCallable = new Callable<String>() {
            public String call() throws Exception {
                return uploadRes(cupidTaskParamPB, planUseResource, submitCupidSession);
            }
        };

        String cupidTaskInfo = RetryUtil.retryFunction(
                uploadResCallable,
                RetryConst.UPLOAD_RESOURCE, 2) +
                "," +
                submitCupidSession.odps.getDefaultProject() +
                "," +
                runningMode;

        java.util.HashMap<String, String> propMap = new java.util.HashMap<String, String>();
        propMap.put("odps.cupid.application.type", submitCupidSession.conf.get("odps.moye.runtime.type", "default"));
        propMap.put("odps.cupid.application.version", submitCupidSession.conf.get("odps.cupid.application.version", "default"));
        propMap.put("biz_id", submitCupidSession.biz_id);
        if (submitCupidSession.flightingMajorVersion != null) {
            propMap.put("odps.task.major.version", submitCupidSession.flightingMajorVersion);
        }

        if (System.getProperty("odps.exec.context.file") != null) {
            String path = System.getProperty("odps.exec.context.file");
            File file = new File(path);
            if (file.exists()) {
                InputStreamReader reader = new InputStreamReader(new FileInputStream(file));
                Gson gson = new GsonBuilder().create();
                HashMap<Object, Object> map = gson.fromJson(reader, new TypeToken<HashMap<Object, Object>>(){}.getType());
                java.util.Map<String, String> smap = (java.util.Map<String, String>) map.get("settings");
                for (Map.Entry<String, String> item : smap.entrySet())
                {
                    propMap.put(item.getKey(), item.getValue());
                }
            } else {
                logger.warn("odps.exec.context.file not exist: " + path);
            }
        }

        if (priority == null)
        {
            return CupidTask.run(submitCupidSession.odps, submitCupidSession.odps.getDefaultProject(), cupidTaskInfo, propMap);
        }
        else
        {
            return CupidTask.run(submitCupidSession.odps, submitCupidSession.odps.getDefaultProject(), cupidTaskInfo, propMap, priority);
        }
    }
}
