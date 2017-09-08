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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;

public abstract class AppMasterLib {

    private String userName = "";
    private String appName = "";
    private static AppMasterLib instance = null;

    protected AppMasterLib() {
    }

    /**
     * AppMasterLib Singleton
     *
     * @return AppMasterLib Instance
     */
    public static AppMasterLib getInstance() {
        if (instance == null) {
            try {
                Class clz = Class.forName("com.aliyun.odps.proxy.fuxi.api.impl.AppMasterLibImpl");
                Constructor<AppMasterLib> meth = (Constructor<AppMasterLib>) clz.getDeclaredConstructor(new Class[]{});
                meth.setAccessible(true);
                instance = meth.newInstance();
            } catch (Exception e) {
                System.err.println("AppMasterLib initialized failed: " + e.toString());
                e.printStackTrace(System.err);
            }
        }
        return instance;
    }

    /**
     * Register a callback function to app master lib for event. <br>
     * If the event has already been registered, the callback function will be overwrite with the new value. <br>
     *
     * @param eventType the event type user wants to register
     * @param callback  the callback function of event
     */
    public abstract void registerEventCallback(EventName eventType, EventCallback callback);

    /**
     * Register a callback function to app master lib to handle user-defined rpc calls. <br>
     *
     * @param callback the callback function of user-defined rpc handling. <br>
     */
    public abstract void registerRpcHandlerCallback(RpcHandlerCallback callback);

    /**
     * require resource in an incremental manner, if role NOT exist, it will be skipped
     *
     * @param resReq map Key:the role name;Value: the incremental resource request
     * @return int
     * @throws IOException
     */
    public abstract int requireResource(HashMap<String, ResourceRequest> resReq) throws IOException;

    /**
     * Incrementally send worker list of scheduling result to app master lib
     * which will really start/stop the workers on tubo. <br>
     * if role NOT exist, it will be skipped. <br>
     *
     * @param workerListMap map Key: the role name; Value: the workers to start/stop
     * @return int
     * @throws IOException
     */
    public abstract int sendWorkerList(HashMap<String, WorkerList> workerListMap) throws IOException;

    /**
     * set a role, its slot id and its resource description
     * warning shouldn't reset a role, otherwise will throw an exception
     *
     * @param roleName roleName
     * @param slotId   slotId
     * @param isStable isStable
     * @param resDesc  resource Desc
     * @return int
     * @throws IOException
     */
    public abstract int addRole(String roleName, long slotId, boolean isStable, HashMap<String, Long> resDesc) throws IOException;

    /**
     * remove a role with roleName
     *
     * @param roleName: role name
     * @return AM_OK for success
     * @throws IOException
     */
    public abstract int removeRole(String roleName) throws IOException;

    public String getUserName() throws IOException {
        if ("".equals(userName)) {
            userName = getUserNameInternal();
        }
        return userName;
    }

    protected abstract String getUserNameInternal() throws IOException;

    public String getAppName() throws IOException {
        if ("".equals(appName)) {
            appName = getAppNameInternal();
        }
        return appName;
    }

    protected abstract String getAppNameInternal() throws IOException;
}
