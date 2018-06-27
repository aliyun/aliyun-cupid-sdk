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

import apsara.odps.cupid.protocol.CupidTaskParamProtos.CupidTaskDetailResultParam;
import apsara.odps.cupid.protocol.CupidTaskParamProtos.CupidTaskOperator;
import apsara.odps.cupid.protocol.CupidTaskParamProtos.CupidTaskParam;
import apsara.odps.cupid.protocol.CupidTaskParamProtos.JobConf;
import apsara.odps.cupid.protocol.CupidTaskParamProtos.JobConfItem;
import apsara.odps.cupid.protocol.CupidTaskParamProtos.OdpsLocalResource;
import apsara.odps.cupid.protocol.CupidTaskParamProtos.OdpsLocalResourceItem;
import apsara.odps.cupid.protocol.VolumePathsCapProtos.PathInfo;
import apsara.odps.cupid.protocol.VolumePathsCapProtos.VolumeCap;
import apsara.odps.cupid.protocol.VolumePathsCapProtos.VolumePaths;
import apsara.odps.cupid.protocol.YarnClientProtos.ApplicationIdProto;
import apsara.odps.cupid.protocol.YarnClientProtos.GetNewApplicationResponseProto;
import apsara.odps.cupid.protocol.YarnClientProtos.ResourceProto;
import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.cupid.CupidConf;
import com.aliyun.odps.cupid.CupidException;
import com.aliyun.odps.cupid.CupidSession;
import com.aliyun.odps.cupid.CupidUtil;
import com.aliyun.odps.cupid.util.TrackUrl;
import com.aliyun.odps.cupid.utils.CupidFSUtil;
import com.aliyun.odps.cupid.utils.SDKConstants;
import com.aliyun.odps.request.cupid.webproxy.PollCallWebProxy;
import com.github.rholder.retry.RetryException;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class YarnClientImplUtil {

    private static Logger logger = Logger.getLogger(YarnClientImplUtil.class);

    public static int getClusterTuboNum()
    {
        // TODO
        // val cupidTaskOperator = CupidTaskOperator.newBuilder()
        // cupidTaskOperator.setMoperator("getclustertubonum")
        // cupidTaskOperator.setMlookupName("")
        // val cupidTaskParamBuilder = CupidTaskParam.newBuilder()
        // cupidTaskParamBuilder.setMcupidtaskoperator(cupidTaskOperator.build())
        // val clusterTuboNumSubmitJob = SubmitJobUtil.getClusterTuboNumSubmitJob(cupidTaskParamBuilder.build())
        // return clusterTuboNumSubmitJob
        return 100;
    }

    public static int getRanDomAppId()
    {
        return new Random().nextInt(Integer.MAX_VALUE) % Integer.MAX_VALUE + 1;
    }

    public static GetNewApplicationResponseProto getNewApplicationResponse()
    {
        GetNewApplicationResponseProto.Builder getNewApplicationResponseProtoBuilder =
                GetNewApplicationResponseProto.newBuilder();
        ApplicationIdProto.Builder applicationIdProtoOrBuilder = ApplicationIdProto.newBuilder();
        int appId = getRanDomAppId();
        applicationIdProtoOrBuilder.setId(appId);
        applicationIdProtoOrBuilder.setClusterTimestamp(System.currentTimeMillis());
        getNewApplicationResponseProtoBuilder.setApplicationId(applicationIdProtoOrBuilder.build());
        ResourceProto.Builder resourceBuilder = ResourceProto.newBuilder();
        resourceBuilder.setMemory(96000);
        resourceBuilder.setVirtualCores(128);
        getNewApplicationResponseProtoBuilder.setMaximumCapability(resourceBuilder.build());
        logger.info("appId " + appId + "," + applicationIdProtoOrBuilder.getClusterTimestamp());
        return getNewApplicationResponseProtoBuilder.build();
    }

    public static void initCupidSession(HashMap<String, String> cupidConfParam)
    {
        CupidConf conf = new CupidConf();
        for (Map.Entry<String, String> cupidConfParamItem : cupidConfParam.entrySet())
        {
            conf.set(cupidConfParamItem.getKey(), cupidConfParamItem.getValue());
        }
        CupidSession.setConf(conf);
    }

    public static CupidTaskDetailResultParam pollAMStatus(Instance ins)
            throws InvalidProtocolBufferException, ExecutionException, RetryException, InterruptedException, CupidException {
        return CupidUtil.getResult(ins);
    }

    public static OdpsLocalResource setProjectForLocalResource(OdpsLocalResource odpsLocalResource)
    {
        return setProjectForLocalResource(odpsLocalResource, null);
    }

    public static OdpsLocalResource setProjectForLocalResource(
            OdpsLocalResource odpsLocalResource,
            CupidSession overrideCupidSession)
    {

        CupidSession cupidSession = overrideCupidSession == null ? CupidSession.get() : overrideCupidSession;

        OdpsLocalResource.Builder odpsLocalResourceBuilder = OdpsLocalResource.newBuilder();
        OdpsLocalResourceItem.Builder odpsLocalResourceBuilderItemBuilder = OdpsLocalResourceItem.newBuilder();

        for (OdpsLocalResourceItem item : odpsLocalResource.getLocalresourceitemList())
        {
            odpsLocalResourceBuilderItemBuilder.setProjectname(cupidSession.conf.get("odps.project.name"));
            odpsLocalResourceBuilderItemBuilder.setRelativefilepath(item.getRelativefilepath());
            odpsLocalResourceBuilderItemBuilder.setType(item.getType());
            odpsLocalResourceBuilder.addLocalresourceitem(odpsLocalResourceBuilderItemBuilder.build());
        }

        return odpsLocalResourceBuilder.build();
    }

    public static String genCupidTrackUrl(
            Instance amInstance,
            String appId,
            String webUrl) throws OdpsException {
        return genCupidTrackUrl(amInstance, appId, webUrl, null);
    }

    public static String genCupidTrackUrl(
            Instance amInstance,
            String appId,
            String webUrl,
            CupidSession overrideCupidSession) throws OdpsException {
        CupidSession cupidSession = overrideCupidSession == null ? CupidSession.get() : overrideCupidSession;

        String endPoint = cupidSession.conf.get("odps.end.point");
        // if contains maxcompute.aliyun means it's public cloud
        String defaultTrackurlHost = endPoint != null && endPoint.toLowerCase().contains("maxcompute.aliyun") ?
            "http://jobview.odps.aliyun.com" :
            "http://jobview.odps.aliyun-inc.com";

        String trackurlHost = cupidSession.conf.get("odps.moye.trackurl.host", defaultTrackurlHost);
        TrackUrl trackUrl = new TrackUrl(cupidSession.odps, trackurlHost);
        String hours = cupidSession.conf.get("odps.moye.trackurl.dutation", "72");
        String cupidType = cupidSession.conf.get("odps.moye.runtime.type", "spark");
        String lookupName = cupidSession.getJobLookupName();
        if (!trackurlHost.equals("")) {
            trackUrl.setLogViewHost(trackurlHost);
        }
        String webproxyEndPoint = cupidSession.conf.get("odps.cupid.webproxy.endpoint", endPoint);
        String trackUrlStr;
        if ("true".equals(cupidSession.conf.get("odps.cupid.proxy.enable", "false"))) {
            trackUrlStr = trackUrl.genCupidTrackUrl(amInstance,
                    appId,
                    webUrl,
                    hours,
                    cupidType,
                    lookupName,
                    webproxyEndPoint,
                    cupidSession
            );
        } else {
            trackUrlStr = trackUrl.genCupidTrackUrl(amInstance,
                    appId,
                    webUrl,
                    hours,
                    cupidType,
                    lookupName,
                    webproxyEndPoint
            );
        }

        if (cupidSession.conf.get("odps.moye.test.callwebproxy", "false").equals("true")) {
            logger.info("start to call the webproxy");
            String interval = cupidSession.conf.get("odps.moye.test.callwebproxy.interval", "10");
            PollCallWebProxy.getInstance().startPoll(trackUrlStr, Integer.parseInt(interval));
        }
        return trackUrlStr;
    }

    public static Instance transformAppCtxAndStartAM(
            HashMap<String, String> paramForFM,
            OdpsLocalResource odpsLocalResource) throws OdpsException, InterruptedException, ExecutionException, InvalidProtocolBufferException, CupidException, RetryException, FileNotFoundException {
        return transformAppCtxAndStartAM(paramForFM, odpsLocalResource, null);
    }

    public static Instance transformAppCtxAndStartAM(
            HashMap<String, String> paramForFM,
            OdpsLocalResource odpsLocalResource,
            CupidSession overrideCupidSession) throws InvalidProtocolBufferException, ExecutionException, RetryException, InterruptedException, CupidException, FileNotFoundException, OdpsException {
        CupidSession cupidSession = overrideCupidSession == null ? CupidSession.get() : overrideCupidSession;

        CupidTaskOperator.Builder cupidTaskOperator = CupidTaskOperator.newBuilder();
        cupidTaskOperator.setMoperator("startam");
        cupidTaskOperator.setMlookupName("");
        String engineType = paramForFM.get(SDKConstants.ENGINE_RUNNING_TYPE);
        int jobPriority =
                paramForFM.get(SDKConstants.CUPID_JOB_PRIORITY) == null ?
                        1 : Integer.parseInt(paramForFM.get(SDKConstants.CUPID_JOB_PRIORITY));
        if (engineType != null) {
            cupidTaskOperator.setMenginetype(engineType);
        }

        // for volume2 capability
        if (paramForFM.containsKey("odps.cupid.volume.paths")) {
            // odps://project1/volume1/,odps://project2/volume2/,odps://project3/volume3/
            // default giving read/write permission
            VolumePaths.Builder volumePathsBuilder = VolumePaths.newBuilder();
            PathInfo.Builder pathInfoBuilder = PathInfo.newBuilder();
            for (String volumePath:paramForFM.get("odps.cupid.volume.paths").split(",")) {
                pathInfoBuilder.setVolumepath(volumePath);
                pathInfoBuilder.setVolumecap(VolumeCap.ReadAndWrite);
                volumePathsBuilder.addPathinfo(pathInfoBuilder.build());
            }
            paramForFM.put(CupidFSUtil.VOLUME_PATH_INFO_KEY, Base64.encodeBase64String(volumePathsBuilder.build().toByteArray()));
        }

        JobConf.Builder jobConfBuilder = JobConf.newBuilder();
        JobConfItem.Builder jobConfItemBuilder = JobConfItem.newBuilder();
        Iterator<Map.Entry<String, String>> iterator = paramForFM.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            if (entry.getKey().startsWith("cupid")
                    || entry.getKey().startsWith("odps")
                    || entry.getKey().startsWith("odps.moye")
                    || entry.getKey().startsWith("odps.executor")
                    || entry.getKey().startsWith("odps.cupid")) {
                jobConfItemBuilder.setKey(entry.getKey());
                jobConfItemBuilder.setValue(entry.getValue());
                jobConfBuilder.addJobconfitem(jobConfItemBuilder.build());
            }
        }

        if (paramForFM.containsKey("odps.cupid.aliyarn.mode.enable") && paramForFM.get("odps.cupid.aliyarn.mode.enable").equals("true")){
            jobConfItemBuilder.setKey("yarn.ipc.client.factory.class");
            jobConfItemBuilder.setValue("org.apache.hadoop.yarn.client.api.aliyarn.FuxiClientFactoryImpl");
            jobConfBuilder.addJobconfitem(jobConfItemBuilder.build());
        } else if (!paramForFM.containsKey("odps.cupid.aliyarn.mode.enable")) {
            jobConfItemBuilder.setKey("odps.cupid.aliyarn.mode.enable");
            jobConfItemBuilder.setValue("true");
            jobConfBuilder.addJobconfitem(jobConfItemBuilder.build());
            jobConfItemBuilder.setKey("yarn.ipc.client.factory.class");
            jobConfItemBuilder.setValue("org.apache.hadoop.yarn.client.api.aliyarn.FuxiClientFactoryImpl");
            jobConfBuilder.addJobconfitem(jobConfItemBuilder.build());
        } else {
            logger.info("User close aliyarn mode!");
        }

        CupidTaskParam.Builder cupidTaskParamBuilder = CupidTaskParam.newBuilder();
        cupidTaskParamBuilder.setMcupidtaskoperator(cupidTaskOperator.build());
        cupidTaskParamBuilder.setJobconf(jobConfBuilder.build());
        cupidTaskParamBuilder.setLocalresource(setProjectForLocalResource(odpsLocalResource));
        boolean cupidPlanUseResource = false;
        if(paramForFM.containsKey("odps.cupid.engine.running.type") && paramForFM.get("odps.cupid.engine.running.type").equals("longtime")){
            cupidPlanUseResource = true;
        }
        Instance instance = SubmitJobUtil.submitJob(cupidTaskParamBuilder.build(), CupidTaskRunningMode.eHasFuxiJob, jobPriority, cupidPlanUseResource, cupidSession);
        logger.info("transformAppCtxAndStartAM instance id " + instance.getId());

        /** gen logview url */
        TrackUrl trackUrl = new TrackUrl(cupidSession.odps);
        String logViewUrl = "http://webconsole.odps.aliyun-inc.com:8080/logview/?h=" + cupidSession.conf.get("odps.end.point") +
                "&p=" + cupidSession.conf.get("odps.project.name") +
                "&i=" + instance.getId() +
                "&token=" + trackUrl.genToken(instance, 72);
        logger.info("logview url: " + logViewUrl);

        cupidSession.setJobLookupName(instance.getId());

        // TODO remove later Remain compatible for Client Mode
        CupidSession.get().setJobLookupName(instance.getId());

        CupidUtil.getResult(instance);
        return instance;
    }
}
