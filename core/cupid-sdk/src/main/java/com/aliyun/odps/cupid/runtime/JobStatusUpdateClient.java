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

package com.aliyun.odps.cupid.runtime;

import java.io.IOException;
import java.lang.reflect.Constructor;

/**
 * Created by liwei.li on 2/13/17.
 */

public abstract class JobStatusUpdateClient {

    private static JobStatusUpdateClient instance = null;

    protected JobStatusUpdateClient() {
    }

    static {
        try {
            Class clz = Class.forName("com.aliyun.odps.cupid.runtime.JobStatusUpdateClientImpl");
            Constructor<JobStatusUpdateClient> meth = (Constructor<JobStatusUpdateClient>) clz.getDeclaredConstructor(new Class[]{});
            meth.setAccessible(true);
            instance = meth.newInstance();
        } catch (Exception e) {
            System.err.println("JobStatusUpdateClient initialized failed: " + e.toString());
            e.printStackTrace();
        }
    }

    public abstract void updateWorkerFinishInfo(String roleName, long endTime) throws IOException;

    public static JobStatusUpdateClient getInstance() {
        return instance;
    }
}