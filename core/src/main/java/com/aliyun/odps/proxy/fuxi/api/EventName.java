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

/* event names for interaction between lib and user code */
public enum EventName {
    /* tubo notifyinfo of a worker's failure */
    EVENT_WORKER_FAILURE(0),

    /**
     * FuxiMaster assign resources to AppMaster through this events
     * AppMaster can only get incremental assignment.
     */
    EVENT_ASSIGNED_RESOURCE_CHANGE(1),

    /* tubo response of SendWorkerList */
    EVENT_SEND_WORKER_LIST_RESPONSE(2),

    /* AM's heartbeat with FuxiMaster timeout */
    EVENT_FM_HEARTBEAT_TIMEOUT(3),
    /* AM receives DisableOldChildMaster message */
    EVENT_DISABLE_OLD_APP_MASTER(4),

    UNKNOWN_EVENT_NAME(5);

    private int value;

    EventName(int value) {
        this.value = value;
    }

    public int value() {
        return this.value;
    }

    public static EventName getEventName(int i) {
        switch (i) {
            case 0:
                return EVENT_WORKER_FAILURE;
            case 1:
                return EVENT_ASSIGNED_RESOURCE_CHANGE;
            case 2:
                return EVENT_SEND_WORKER_LIST_RESPONSE;
            case 3:
                return EVENT_FM_HEARTBEAT_TIMEOUT;
            case 4:
                return EVENT_DISABLE_OLD_APP_MASTER;
            default:
                return null;
        }
    }
}
