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

package com.aliyun.odps.proxy.nuwa.api;

import java.util.Map;

public class NuwaSubscribedItemStatus {
    private boolean isExist;
    private Map<String, byte[]> items;

    public NuwaSubscribedItemStatus(boolean isExist, Map<String, byte[]> items) {
        this.isExist = isExist;
        this.items = items;
    }

    public boolean isExist() {
        return isExist;

    }

    public Map<String, byte[]> getItems() {
        return items;
    }

    @Override
    public String toString() {
        return "NuwaSubscribedItemStatus{" +
                "isExist=" + isExist +
                ", items=" + items +
                '}';
    }
}