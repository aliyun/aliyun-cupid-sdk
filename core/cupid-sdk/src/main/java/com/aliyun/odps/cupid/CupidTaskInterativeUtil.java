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
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.cupid.requestcupid.CupidTaskBaseUtil;
import com.aliyun.odps.cupid.requestcupid.CupidTaskOperatorConst;
import com.aliyun.odps.cupid.requestcupid.CupidTaskRunningMode;
import com.aliyun.odps.cupid.requestcupid.SubmitJobUtil;
import com.aliyun.odps.cupid.utils.JTuple;
import com.github.rholder.retry.RetryException;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.util.concurrent.ExecutionException;

public class CupidTaskInterativeUtil {

    private static Logger logger = Logger.getLogger(CupidTaskInterativeUtil.class);

    public static JTuple.JTuple2<String, String> getSaveTempDirAndCapFile(
            String lookupName,
            String projectName,
            String tableName,
            String saveId) throws InvalidProtocolBufferException, ExecutionException, RetryException, InterruptedException, CupidException, OdpsException, FileNotFoundException {
        return getSaveTempDirAndCapFile(lookupName, projectName, tableName, saveId, 9);
    }

    public static JTuple.JTuple2<String, String> getSaveTempDirAndCapFile(
            String lookupName,
            String projectName,
            String tableName,
            String saveId,
            int priority) throws InvalidProtocolBufferException, ExecutionException, RetryException, CupidException, InterruptedException, OdpsException, FileNotFoundException {

        CupidTaskParam.Builder moyeTaskParamBuilder =
                CupidTaskBaseUtil.getOperationBaseInfo(
                        CupidSession.get(),
                        CupidTaskOperatorConst.CUPID_TASK_GET_SAVETEMPDIR,
                        CupidSession.get().getJobLookupName(),
                        null
                );

        SaveTableInfo.Builder saveTableInfoBuilder = SaveTableInfo.newBuilder();
        saveTableInfoBuilder.setProjectname(projectName);
        saveTableInfoBuilder.setTablename(tableName);
        saveTableInfoBuilder.setSaveid(saveId);
        moyeTaskParamBuilder.setSaveTableInfo(saveTableInfoBuilder.build());

        CupidTaskParam moyeTaskParam = moyeTaskParamBuilder.build();
        Instance instance = SubmitJobUtil.submitJob(moyeTaskParam, CupidTaskRunningMode.eAsyncNotFuxiJob, priority);
        logger.info("get SaveTempDirAndCapFile instance:" + instance.getId());
        String[] SaveTempDirAndCapFileResult = CupidUtil.pollSuccessResult(instance, "get save project temp dir...").split(",");
        return JTuple.tuple(SaveTempDirAndCapFileResult[0], SaveTempDirAndCapFileResult[1]);
    }
}
