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

import java.util.HashMap;

public class OverUsedWorkItem {
    private long iKilledTime;
    private String killedTime;
    private HashMap<String, Long> plannedResource;
    private HashMap<String, Long> usedResource;

    public OverUsedWorkItem() {
        plannedResource = new HashMap<String, Long>();
        usedResource = new HashMap<String, Long>();
    }

    public long getiKilledTime() {
        return iKilledTime;
    }

    public void setiKilledTime(long iKilledTime) {
        this.iKilledTime = iKilledTime;
    }

    public String getKilledTime() {
        return killedTime;
    }

    public void setKilledTime(String killedTime) {
        this.killedTime = killedTime;
    }

    public HashMap<String, Long> getPlannedResource() {
        return plannedResource;
    }

    public HashMap<String, Long> getUsedResource() {
        return usedResource;
    }

    public void addPlannedResource(String resource, long count) {
        plannedResource.put(resource, count);
    }

    public void addUsedResource(String resource, long count) {
        usedResource.put(resource, count);
    }

    @Override
    public String toString() {
        return "iKilledTime=>" + iKilledTime + "\nkilledTime=>" + killedTime
                + "\nplannedResource=>" + plannedResource + "\nusedResource=>" + usedResource;
    }
    //private plannedResource usedResource;
}
