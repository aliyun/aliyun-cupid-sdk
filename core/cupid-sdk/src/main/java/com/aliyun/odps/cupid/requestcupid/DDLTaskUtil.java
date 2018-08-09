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
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.cupid.CupidSession;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Set;

public class DDLTaskUtil {

    private static Logger logger = Logger.getLogger(DDLTaskUtil.class);

    private static CupidTaskParam.Builder getMoyeTaskParamInfo()
    {
        CupidTaskParam.Builder cupidTaskParamBuilder =
                CupidTaskBaseUtil.getOperationBaseInfo(
                        CupidSession.get(),
                        CupidTaskOperatorConst.CUPID_TASK_DDLTASK,
                        CupidSession.get().getJobLookupName(),
                        null
                );

        return cupidTaskParamBuilder;
    }

    public static String ParsePartSpec(String partSpecIn)
    {
        PartitionSpec parsePartSpec = new PartitionSpec(partSpecIn);
        String parsePartOut = parsePartSpec.toString().trim().replaceAll("'", "");
        return parsePartOut;
    }

    // TODO might need to be public
    private static DDLInfo.Builder getDDLTaskBaseInfo(String project,
                                                      String table,
                                                      boolean isOverWrite)
    {
        DDLInfo.Builder ddlInfoBuilder = DDLInfo.newBuilder();
        ddlInfoBuilder.setSaveTableProject(project);
        ddlInfoBuilder.setSaveTableName(table);
        ddlInfoBuilder.setIsOverWrite(isOverWrite);
        return ddlInfoBuilder;
    }

    private static void addDDLInfoItermForSaveMultiParittion(DDLInfo.Builder ddlInfoBuilder,
                                                             Set<String> ddlTaskPaths)
    {
        String[] ddlTaskPathsArray = ddlTaskPaths.toArray(new String[ddlTaskPaths.size()]);
        DDLInfoIterm.Builder ddlInfoItermBuilder = DDLInfoIterm.newBuilder();
        ArrayList<String> outPutPartitionInfo = new ArrayList<String>();
        for (String ddlTaskPath : ddlTaskPathsArray)
        {
            String[] pathParams = ddlTaskPath.split("/");
            String pathPectOut = ParsePartSpec(pathParams[pathParams.length - 1]);
            ddlInfoItermBuilder.setPanguTempDirPath(ddlTaskPath);
            ddlInfoItermBuilder.setPartSpec(pathPectOut);
            if (outPutPartitionInfo.size() < 3) {
                outPutPartitionInfo.add(pathPectOut.toString());
            }
            ddlInfoBuilder.addDdlInfoIterms(ddlInfoItermBuilder.build());
        }
        String outputPartitionMesg = "the saved partition num = " + ddlTaskPathsArray.length + ", show some partition" + "\n";
        for (String f : outPutPartitionInfo)
        {
            outputPartitionMesg = outputPartitionMesg + f + "\n";
        }
        logger.info(outputPartitionMesg);
    }

    public static void doDDLTaskForSaveMultiParittion(String project,
                                                      String table,
                                                      boolean isOverWrite,
                                                      Set<String> ddlTaskPaths) throws Exception {
        DDLInfo.Builder ddlInfoBuilder = getDDLTaskBaseInfo(project, table, isOverWrite);
        addDDLInfoItermForSaveMultiParittion(ddlInfoBuilder, ddlTaskPaths);
        CupidTaskParam.Builder moyeTaskParamBuilder = getMoyeTaskParamInfo();
        moyeTaskParamBuilder.setDdlInfo(ddlInfoBuilder.build());
        SubmitJobUtil.ddlTaskSubmitJob(moyeTaskParamBuilder.build());
    }

    public static void doDDLTaskForSaveMultiTables(DDLMultiTableInfos ddlMultiTables) throws Exception {
        CupidTaskParam.Builder moyeTaskParamBuilder = getMoyeTaskParamInfo();
        moyeTaskParamBuilder.setDdlMultiTableInfos(ddlMultiTables);
        SubmitJobUtil.ddlTaskSubmitJob(moyeTaskParamBuilder.build());
    }

    public static void doDDLTaskForNoarmalSave(String project,
                                               String table,
                                               String partSpecOut,
                                               boolean isOverWrite,
                                               String panguTempDirPath) throws Exception {
        DDLInfo.Builder ddlInfoBuilder = getDDLTaskBaseInfo(project, table, isOverWrite);
        DDLInfoIterm.Builder ddlInfoItermBuilder = DDLInfoIterm.newBuilder();
        ddlInfoItermBuilder.setPanguTempDirPath(panguTempDirPath);
        ddlInfoItermBuilder.setPartSpec(partSpecOut);
        ddlInfoBuilder.addDdlInfoIterms(ddlInfoItermBuilder.build());

        CupidTaskParam.Builder moyeTaskParamBuilder = getMoyeTaskParamInfo();

        moyeTaskParamBuilder.setDdlInfo(ddlInfoBuilder.build());
        SubmitJobUtil.ddlTaskSubmitJob(moyeTaskParamBuilder.build());
    }
}
