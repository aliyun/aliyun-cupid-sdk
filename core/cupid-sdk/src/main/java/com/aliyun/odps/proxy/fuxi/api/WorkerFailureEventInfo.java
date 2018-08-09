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

public class WorkerFailureEventInfo extends EventInfo {
  /* KILL_PLAN_DISCARD and KILL_PREEMPT */
  private ArrayList<WorkerFailureEntry> killedByTuboList;

  /* CRASH */
  private ArrayList<WorkerFailureEntry> crashedList;

  /* KILL_OOM */
  private HashMap<String, OverUsedWorkItem> killedOverUsedList;

  public WorkerFailureEventInfo() {
    killedByTuboList = new ArrayList<WorkerFailureEntry>();
    crashedList = new ArrayList<WorkerFailureEntry>();
    killedOverUsedList = new HashMap<String, OverUsedWorkItem>();
  }

  public void addKilledByTuboEntry(WorkerFailureEntry entry) {
    killedByTuboList.add(entry);
  }

  public void addCrashedEntry(WorkerFailureEntry entry) {
    crashedList.add(entry);
  }

  public void addOverUsedEntry(String workerName, OverUsedWorkItem item) {
    killedOverUsedList.put(workerName, item);
  }

  public ArrayList<WorkerFailureEntry> getKilledByTuboList() {
    return killedByTuboList;
  }

  public ArrayList<WorkerFailureEntry> getCrashedList() {
    return crashedList;
  }

  public HashMap<String, OverUsedWorkItem> getKilledOverUsedList() {
    return killedOverUsedList;
  }

  @Override
  public String toString() {
    return "killedByTuboList=>" + killedByTuboList + "\n crashedList=>"
            + crashedList + "\n killedOverUsedList=>" + killedOverUsedList;
  }

}
