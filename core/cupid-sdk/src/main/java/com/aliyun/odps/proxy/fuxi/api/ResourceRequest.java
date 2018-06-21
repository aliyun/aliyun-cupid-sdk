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

public class ResourceRequest {
  private HashMap<ResourceLocation, Integer> request;
  private HashMap<String, Integer> release;
  private HashMap<ResourceLocation, Boolean> blackList;
  private int maxSlotCount;

  public ResourceRequest() {
    request = new HashMap<ResourceLocation, Integer>();
    release = new HashMap<String, Integer>();
    blackList = new HashMap<ResourceLocation, Boolean>();
  }

  public HashMap<ResourceLocation, Integer> getRequest() {
    return request;
  }

  public HashMap<String, Integer> getRelease() {
    return release;
  }

  public HashMap<ResourceLocation, Boolean> getBlackList() {
    return blackList;
  }

  public int getMaxSlotCount() {
    return maxSlotCount;
  }

  public void require(ResourceLocation location, int count) {
    if (request.containsKey(location)) {
      count += request.get(location);
    }
    request.put(location, count);
  }

  public void release(String machine, int count) {
    if (release.containsKey(machine)) {
      count += release.get(machine);
    }
    release.put(machine, count);
  }

  public void refuse(ResourceLocation location, boolean add) {
    blackList.put(location, add);
  }

  public void setMaxSlotCount(int slotCount) {
    this.maxSlotCount = slotCount;
  }
}
