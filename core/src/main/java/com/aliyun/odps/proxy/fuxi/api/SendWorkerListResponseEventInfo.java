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

import java.util.ArrayList;

public class SendWorkerListResponseEventInfo extends EventInfo {
    protected ArrayList<String> startWorkerList;
    protected ArrayList<String> stopWorkerList;

    public SendWorkerListResponseEventInfo() {
        startWorkerList = new ArrayList<String>();
        stopWorkerList = new ArrayList<String>();
    }

    public void start(String machineName) {
        startWorkerList.add(machineName);
    }

    public void stop(String machineName) {
        stopWorkerList.add(machineName);
    }

    public ArrayList<String> getStartWorkerList() {
        return startWorkerList;
    }

    public ArrayList<String> getStopWorkerList() {
        return stopWorkerList;
    }

    public void setStartWorkerList(ArrayList<String> startWorkerList) {
        this.startWorkerList = startWorkerList;
    }

    public void setStopWorkerList(ArrayList<String> stopWorkerList) {
        this.stopWorkerList = stopWorkerList;
    }

    @Override
    public String toString() {
        return "startWorkerList=>" + startWorkerList + "\n stopWorkerList=>" + stopWorkerList + "\n";
    }
}
