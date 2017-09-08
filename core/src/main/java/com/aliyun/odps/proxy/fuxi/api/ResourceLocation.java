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

public class ResourceLocation {
    public static final int LT_MACHINE = 1;
    public static final int LT_RACK = 2;
    public static final int LT_ENGINEROOM = 3;
    public static final int LT_CLUSTER = 4;

    int type;
    String name;   // machine name, rack name, cluster name correspondingly

    public ResourceLocation(int t, String name) {
        this.type = t;
        this.name = name;
    }

    public int getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object obj) {
        return type == ((ResourceLocation) obj).type && name.equals(((ResourceLocation) obj).name);
    }

    @Override
    public String toString() {
        String strType;
        switch (type) {
            case LT_MACHINE:
                strType = "LT_MACHINE";
                break;
            case LT_RACK:
                strType = "LT_RACK";
                break;
            case LT_ENGINEROOM:
                strType = "LT_ENGINEROOM";
                break;
            case LT_CLUSTER:
                strType = "LT_CLUSTER";
                break;
            default:
                strType = "UNKNOWN";
                break;
        }
        return strType + ":" + name;
    }
}
