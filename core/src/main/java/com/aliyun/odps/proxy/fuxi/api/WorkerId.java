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

package com.aliyun.odps.proxy.fuxi.api;

public class WorkerId {
    public String userName;
    public String appName;
    public String roleName;
    public String machineName;
    public long workerIndex;

    public WorkerId(String userName, String appName, String roleName,
                    String machineName, long workerIndex) {
        this.userName = userName;
        this.appName = appName;
        this.roleName = roleName;
        this.machineName = machineName;
        this.workerIndex = workerIndex;
    }

    @Override
    public String toString() {
        return userName + "/" + appName + "/" + roleName + "@" + machineName + "#" + workerIndex;
    }

    public static WorkerId fromString(String workerId) {
        String[] tuple = workerId.split("/|@|#");
        if (tuple.length != 5) {
            throw new RuntimeException("Illegal workId=>" + workerId);
        }

        long workerIndex = Long.parseLong(tuple[4]);

        return new WorkerId(tuple[0], tuple[1], tuple[2], tuple[3], workerIndex);
    }
}
