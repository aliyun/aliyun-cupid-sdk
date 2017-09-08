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

import java.io.Serializable;
import java.util.HashMap;

/*
 *  @name : struct WorkerProcessInfo
 *  @brief : maintain workers' metadata
 *  @members :
 *      executableUri :
 *      worker binary path in PackageManager
 *      e.g. "package://aos_fuxi_example/fuxi_dummy_worker"
 *
 *      packageCapability :
 *      capability string for Tubo to download worker binary from PackageManager
 *
 *      params :
 *      these parameters will be passed to Tubo. Tubo will use some of them to
 *      start the worker, and the worker can get the rest of them in the code.
 *      See app master lib's wiki to get the reserved parameters for Tubo.
 *
 *      resource :
 *      the amount of resource a worker needs to be executed
 *      e.g. {"CPU": 50, "Memory": 40}
 *
 *      slotId :
 *      slotId is an integer to identify a ResourceDescription, slotId shouldn't
 *      be reused.
 *      
 *      resHardLimitRatio : 
 *      cgroups related parameter, this worker'll be killed by cgroups
 *      if this worker uses more than resHardLimitRatio * resource
 */
public class WorkerProcessInfo implements Serializable {
    private static final long serialVersionUID = 9135922462320653825L;
    String executableUri;
    String packageCapability;
    HashMap<String, String> params;
    HashMap<String, Long> resource;
    long slotId;

    public void setSlotId(long slotId) {
        this.slotId = slotId;
    }

    public long getSlotId() {
        return slotId;
    }

    double resHardLimitRatio;

    public String getExecutableUri() {
        return executableUri;
    }

    public void setExecutableUri(String executableUri) {
        this.executableUri = executableUri;
    }

    public String getPackageCapability() {
        return packageCapability;
    }

    public void setPackageCapability(String packageCapability) {
        this.packageCapability = packageCapability;
    }

    public HashMap<String, String> getParams() {
        return params;
    }

    public void setParams(HashMap<String, String> params) {
        this.params = params;
    }

    public HashMap<String, Long> getResource() {
        return resource;
    }

    public void setResource(HashMap<String, Long> resource) {
        this.resource = resource;
    }

    public double getResHardLimitRatio() {
        return resHardLimitRatio;
    }

    public void setResHardLimitRatio(double resHardLimitRatio) {
        this.resHardLimitRatio = resHardLimitRatio;
    }

    public WorkerProcessInfo() {
        this.resHardLimitRatio = 1.0;
        //other is null;
    }

    public WorkerProcessInfo(String uri, String cap, HashMap<String, String> params,
                             HashMap<String, Long> resource, double ratio) {
        this.executableUri = uri;
        this.packageCapability = cap;
        this.params = params;
        this.resource = resource;
        this.resHardLimitRatio = ratio;
    }

    @Override
    public String toString() {
        return "executableUri=>" + executableUri
                + "\npackageCapability=>" + packageCapability
                + "\nparams=>" + params
                + "\nresource=>" + resource
                + "\nresHardLimitRatio=>" + resHardLimitRatio + "\n";
    }
}
