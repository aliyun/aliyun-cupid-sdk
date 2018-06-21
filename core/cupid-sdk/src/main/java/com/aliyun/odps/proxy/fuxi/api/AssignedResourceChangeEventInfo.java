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

public class AssignedResourceChangeEventInfo extends EventInfo {

  /* @brief FuxiMaster's assignment
   * <role, <machine, count>>
   * @notice
   * if count > 0, FuxiMaster assigns resources to AppMaster
   * if count < 0, FuxiMaster deprives resources from AppMaster enforcedly
   */
  protected HashMap<String, HashMap<String, Integer>> assignment;
  protected HashMap<String, ArrayList<String>> timeoutMachineList;
  protected HashMap<String, ArrayList<String>> aliveMachineList;

  protected HashMap<String, ArrayList<String>> incrStableMachineInClusterBlacklist;
  protected HashMap<String, ArrayList<String>> decrStableMachineInClusterBlacklist;

  public AssignedResourceChangeEventInfo() {
    assignment = new HashMap<String, HashMap<String, Integer>>();
    timeoutMachineList = new HashMap<String, ArrayList<String>>();
    aliveMachineList = new HashMap<String, ArrayList<String>>();
    decrStableMachineInClusterBlacklist = new HashMap<String, ArrayList<String>>();
    incrStableMachineInClusterBlacklist = new HashMap<String, ArrayList<String>>();
  }

  public void assign(String roleName, String machineName, int count) {
    HashMap<String, Integer> roleAssignment = assignment.get(roleName);
    if (roleAssignment == null) {
      roleAssignment = new HashMap<String, Integer>();
      assignment.put(roleName, roleAssignment);
    }

    roleAssignment.put(machineName, count);
  }

  public void addTimeoutMachine(String roleName, String machine) {
    ArrayList<String> machineList = timeoutMachineList.get(roleName);
    if (machineList == null) {
      machineList = new ArrayList<String>();
      timeoutMachineList.put(roleName, machineList);
    }
    machineList.add(machine);
  }

  public void addAliveMachine(String roleName, String machine) {
    ArrayList<String> machineList = aliveMachineList.get(roleName);
    if (machineList == null) {
      machineList = new ArrayList<String>();
      aliveMachineList.put(roleName, machineList);
    }
    machineList.add(machine);
  }

  public void incrStableMachineInClusterBlacklist(String roleName, String machine) {
    ArrayList<String> machineList = incrStableMachineInClusterBlacklist.get(roleName);
    if (machineList == null) {
      machineList = new ArrayList<String>();
      incrStableMachineInClusterBlacklist.put(roleName, machineList);
    }
    machineList.add(machine);
  }

  public void decrStableMachineInClusterBlacklist(String roleName, String machine) {
    ArrayList<String> machineList = aliveMachineList.get(roleName);
    if (machineList == null) {
      machineList = new ArrayList<String>();
      decrStableMachineInClusterBlacklist.put(roleName, machineList);
    }
    machineList.add(machine);
  }

  public HashMap<String, HashMap<String, Integer>> getAssignment() {
    return assignment;
  }

  public HashMap<String, ArrayList<String>> getTimeoutMachineList() {
    return timeoutMachineList;
  }

  public HashMap<String, ArrayList<String>> getIncrStableMachineInClusterBlacklist() {
    return this.incrStableMachineInClusterBlacklist;
  }

  public HashMap<String, ArrayList<String>> getDecrStableMachineInClusterBlacklist() {
    return this.decrStableMachineInClusterBlacklist;
  }

  public HashMap<String, ArrayList<String>> getAliveMachineList() {
    return aliveMachineList;
  }

  @Override
  public String toString() {
    return "assignment=>" + assignment + "timeoutMachineList" + timeoutMachineList
            + "aliveMachineList" + aliveMachineList + "\n";
  }

}