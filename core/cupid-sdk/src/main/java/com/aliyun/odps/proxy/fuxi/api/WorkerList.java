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
import java.util.HashMap;

public class WorkerList {
  private HashMap<String, WorkerProcessInfo> startList;
  private ArrayList<String> stopList;

  public WorkerList() {
    startList = new HashMap<String, WorkerProcessInfo>();
    stopList = new ArrayList<String>();
  }

  public HashMap<String, WorkerProcessInfo> getStartList() {
    return startList;
  }

  public ArrayList<String> getStopList() {
    return stopList;
  }

  public void start(String workerName, WorkerProcessInfo procInfo) {
    startList.put(workerName, procInfo);
  }

  public void start(String workerName) {
    startList.put(workerName, new WorkerProcessInfo());
  }

  public void stop(String workerName) {
    stopList.add(workerName);
  }

  @Override
  public String toString() {
    return "startList:" + startList.toString() + "stopList: " + stopList.toString();
  }
}
