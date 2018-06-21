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

public class CupidTaskOperatorConst {
    public static String CUPID_TASK_INIT = "DownloadPlan";
    public static String CUPID_TASK_START = "StartAM";
    public static String CUPID_TASK_FAILED = "Failed";
    public static String CUPID_TASK_TERMINATED = "Terminated";
    public static String CUPID_TASK_CANCELLED = "Cancelled";
    public static String CUPID_TASK_GETPARTITIONSIZE = "GetPartitionSize";
    public static String CUPID_TASK_GENVOLUMEPANGUPATH = "GenVolumePanguPath";
    public static String CUPID_TASK_GET_SAVETEMPDIR = "getSaveTempDir";
    public static String CUPID_TASK_COPY_TEMPRESOURCE = "CopyTempResourceToFuxiTempDir";
    public static String CUPID_TASK_DDLTASK = "DoDDLTask";
    public static String CUPID_TASK_GET_APPLICATION_META = "GetApplicationMeta";
    public static String CUPID_TASK_CREATE_APPLICATION_META = "CreateApplicationMeta";
    public static String CUPID_TASK_LIST_APPLICATION_META = "ListApplicationMeta";
    public static String CUPID_TASK_UPDATE_APPLICATION_META = "UpdateApplicationMeta";
    public static String CUPID_TASK_SET_INFORMATION = "SetCupidInformation";
    public static String CUPID_TASK_GET_PROXY_TOKEN = "GetCupidProxyToken";
    public static String CUPID_TASK_TASK_SERVICE_REQUEST = "TaskServiceRequest";
    public static String CUPID_TASK_UPGRADE_APPLICATION = "UpgradeApplication";
}
