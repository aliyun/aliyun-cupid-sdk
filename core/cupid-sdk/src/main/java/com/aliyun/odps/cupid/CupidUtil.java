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

package com.aliyun.odps.cupid;

import apsara.odps.cupid.protocol.CupidTaskParamProtos.*;
import com.aliyun.odps.Instance;
import com.aliyun.odps.LogView;
import com.aliyun.odps.cupid.requestcupid.RetryConst;
import com.aliyun.odps.cupid.requestcupid.RetryUtil;
import com.github.rholder.retry.RetryException;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class CupidUtil {

    private static Logger logger = Logger.getLogger(CupidUtil.class);

    public static CupidException errMsg2SparkException(String errMsg)
    {
        if (errMsg.startsWith("runTask failed:") || errMsg.startsWith("app run failed!")) {
            return new UserException(errMsg);
        } else {
            return new CupidException(errMsg);
        }
    }

    public static String getTaskDetailJson(final Instance ins) throws ExecutionException, RetryException {
        Callable<String> getTaskDetailJsonCallable = new Callable<String>() {
            public String call() throws Exception {
                return ins.getTaskDetailJson("cupid_task");
            }
        };

        return RetryUtil.retryFunction(
                getTaskDetailJsonCallable,
                RetryConst.GET_TASK_DETAIL_JSON,
                2
        );
    }

    public static CupidTaskDetailResultParam getInsStatus(Instance ins)
            throws ExecutionException, RetryException, InstanceRecycledException, InvalidProtocolBufferException {
        String detailResult = getTaskDetailJson(ins);
        if (detailResult.equals(InstanceRecycledException.InstanceRecycledMsg)) {
            throw new InstanceRecycledException(detailResult);
        }

        CupidTaskDetailResultParam taskDetailResultParam =
                CupidTaskDetailResultParam.parseFrom(Base64.decodeBase64(detailResult.getBytes()));
        if (taskDetailResultParam.hasReady() ||
                taskDetailResultParam.hasWaiting() ||
                taskDetailResultParam.hasRunning() ||
                taskDetailResultParam.hasSuccess() ||
                taskDetailResultParam.hasFailed() ||
                taskDetailResultParam.hasCancelled() ||
                taskDetailResultParam.hasWaitForReRun())
        {
            return taskDetailResultParam;
        }
        else
        {
            logger.debug("taskDetailResultParam is empty, set Ready!");
            return taskDetailResultParam.toBuilder().setReady(Ready.newBuilder().build()).build();
        }
    }

    public static String pollSuccessResult(Instance ins, String msg)
            throws InvalidProtocolBufferException, ExecutionException, RetryException, InterruptedException, CupidException {
        boolean _break = false;
        while (!_break)
        {
            CupidTaskDetailResultParam re = getResult(ins);
            if (re.hasFailed()) {
                if (re.getFailed().hasBizFailed()) {
                    throw errMsg2SparkException(re.getFailed().getBizFailed().getBizFailedMsg());
                }
            } else if(re.hasCancelled()){
                throw new UserException("instance be cancelled");
            } else if (re.hasRunning()) {
                logger.info(msg);
                Thread.sleep(1000);
            } else if (re.hasSuccess()) {
                return re.getSuccess().getSuccessMsg();
            } else {
                if (re.toString() != "") {
                    // empty case means the cupid task not set TaskDetail
                    logger.info("unexpected status: " + re.toString());
                }

                Thread.sleep(1000);
            }
        }
        return "";
    }

    public static CupidTaskDetailResultParam getResult(Instance ins)
            throws InvalidProtocolBufferException, ExecutionException, RetryException, CupidException, InterruptedException {
        CupidTaskDetailResultParam result = null;
        boolean _break = false;
        while (!_break) {
            result = getInsStatus(ins);
            if (result.hasFailed() && result.getFailed().hasCupidTaskFailed()) {
                throw errMsg2SparkException(result.getFailed().getCupidTaskFailed().getCupidTaskFailedMsg());
            } else if (result.hasReady()) {
                logger.info("ready!!!");
                Thread.sleep(1000);
            } else if (result.hasWaiting()) {
                logger.info("waiting!!!");
                Thread.sleep(1000);
            } else if (result.hasWaitForReRun()) {
                String reRunMsg = result.getWaitForReRun().getWaitMsg();
                logger.info("waiting for ReRun, instanceId: " + ins.getId() + ", Msg: " + reRunMsg + "!!!");
                Thread.sleep(30000);
            } else {
                _break = true;
            }
        }
        return result;
    }

    public static String getLogViewUrl(final Instance instance) throws ExecutionException, RetryException {
        final LogView logViewHandler = new LogView(CupidSession.get().odps);
        Callable<String> generateLogViewCallable = new Callable<String>() {
            public String call() throws Exception {
                return logViewHandler.generateLogView(instance, 7*24);
            }
        };

        String logViewUrl = RetryUtil.retryFunction(
                generateLogViewCallable,
                RetryConst.GET_LOG_VIEW_URL, 60);
        return logViewUrl;
    }

    public static String getEngineLookupName()
    {
        return CupidSession.get().getJobLookupName();
    }
}
