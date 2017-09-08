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

package com.aliyun.odps.cupid.interaction;

import com.aliyun.odps.cupid.utils.SDKConstants;

import java.net.URI;
import java.net.URISyntaxException;

public class InteractionClientFactory {
    public static InteractionClient getAppClient(String protocol, String hostWithPort, String token) {

        if (Protocol.WEBSOCKET.name().equalsIgnoreCase(protocol)) {
            WebSocketClient clent;
            try {
                clent =
                        new WebSocketClient(new URI(SDKConstants.CUPID_INTERACTION_WEBSOCKET_SCHEMA
                                + hostWithPort + SDKConstants.CUPID_INTERACTION_WEBSOCKET_PATH),
                                SDKConstants.CUPID_INTERACTION_SUB_PROTOCOL_APP, token,
                                SDKConstants.INTERACTION_CLIENT_INPUT_MODE_INPUTSTREAM);
                return clent;
            } catch (URISyntaxException e) {
                throw new RuntimeException(hostWithPort + " is a invalid host!");
            }
        } else {
            throw new RuntimeException(protocol + " is not supported now!");
        }
    }

    public static InteractionClient getUserClient(String protocol, String proxyEndpoint) {

        if (Protocol.WEBSOCKET.name().equalsIgnoreCase(protocol)) {
            WebSocketClient clent;
            try {
                clent =
                        new WebSocketClient(new URI(proxyEndpoint),
                                SDKConstants.CUPID_INTERACTION_SUB_PROTOCOL_CLIENT, null,
                                SDKConstants.INTERACTION_CLIENT_INPUT_MODE_INPUTSTREAM);
                return clent;
            } catch (URISyntaxException e) {
                throw new RuntimeException("Invalid endpoint:" + proxyEndpoint);
            }
        } else {
            throw new RuntimeException(protocol + " is not supported now!");
        }
    }
}
