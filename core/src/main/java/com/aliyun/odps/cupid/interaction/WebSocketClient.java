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
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileDescriptor;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpCookie;
import java.net.URI;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class WebSocketClient implements InteractionClient {

    private static final Logger LOG = LoggerFactory.getLogger(WebSocketClient.class);

    private InteractionSocket interactionSocket;

    private URI destUri;

    private String subProtocol;

    private String token;

    private String routeInfo;

    private String loadBalanceHashKey;

    private org.eclipse.jetty.websocket.client.WebSocketClient client;

    WebSocketClient(URI destUri, String subProtocol, String token, int inputMode) {
        this.destUri = destUri;
        this.subProtocol = subProtocol;
        this.token = token;
        interactionSocket = new InteractionSocket(this, inputMode);
        connect(false);
    }

    public void connect(boolean isReconnect) {
        if (client == null) {
            client = new org.eclipse.jetty.websocket.client.WebSocketClient();
        }
        // For debug usage
        // client.setConnectTimeout(SDKConstants.ONE_HOUR);
        try {
            if (!client.isStarted()) {
                client.start();
            }
            ClientUpgradeRequest request = new ClientUpgradeRequest();
            request.setSubProtocols(subProtocol);
            setLoadBalanceKey(request);
            if (token != null) {
                request.setHeader(SDKConstants.CUPID_INTERACTION_HEADER_TOKEN, token);
            }
            if (isReconnect) {
                request.setHeader(SDKConstants.CUPID_INTERACTION_HEADER_RECONNECT,
                        String.valueOf(System.currentTimeMillis()));
            }
            LOG.info(subProtocol + " - Connecting to : " + destUri);
            Future<Session> future = client.connect(interactionSocket, destUri, request);
            Session session = future.get(5l, TimeUnit.SECONDS);
            if (!isReconnect && SDKConstants.CUPID_INTERACTION_SUB_PROTOCOL_CLIENT.equals(subProtocol)) {
                // will be used for reconnect
                token = session.getUpgradeResponse().getHeader(SDKConstants.CUPID_INTERACTION_HEADER_TOKEN);
                routeInfo =
                        session.getUpgradeResponse()
                                .getHeader(SDKConstants.CUPID_INTERACTION_HEADER_ROUTE_INFO);
            }
            extractLoadBalanceKey(session);
            LOG.info(subProtocol + " - Connected!");
        } catch (Throwable t) {
            String errMsg = subProtocol + " - Websocket connect failed";
            LOG.error(errMsg, t);
            close();
        }
    }

    private void setLoadBalanceKey(ClientUpgradeRequest request) {
        if (SDKConstants.CUPID_INTERACTION_SUB_PROTOCOL_CLIENT.equals(subProtocol)
                && loadBalanceHashKey != null) {
            // add hash_key cookie for load balance
            int cookieVersion = 0;
            String lbKey = loadBalanceHashKey;
            if (loadBalanceHashKey.contains("\"")) {
                cookieVersion = 1;
                lbKey = lbKey.replaceAll("\"", "");
            }
            HttpCookie cookie = new HttpCookie(SDKConstants.CUPID_INTERACTION_COOKIE_HASH_KEY, lbKey);
            cookie.setVersion(cookieVersion);
            request.getCookies().add(cookie);
        }
    }

    private void extractLoadBalanceKey(Session session) {
        String setCookieValue =
                session.getUpgradeResponse().getHeader(SDKConstants.CUPID_INTERACTION_HEADER_SET_COOKIE);
        if (setCookieValue != null) {
            setCookieValue = setCookieValue.trim();
            String[] kv = setCookieValue.split(";");
            for (String kvStr : kv) {
                if (kvStr.contains("=")) {
                    String[] kAndV = kvStr.split("=");
                    if (kAndV.length == 2 && kAndV[0] != null && kAndV[1] != null) {
                        if (SDKConstants.CUPID_INTERACTION_COOKIE_HASH_KEY.equals(kAndV[0].trim())) {
                            loadBalanceHashKey = kAndV[1].trim();
                            LOG.info(subProtocol + " - loadbalance key:" + loadBalanceHashKey);
                            return;
                        }
                    }
                }
            }
        }
    }

    public InputStream getInputStream() {
        return interactionSocket.getInputStream();
    }

    public void setInput(FileDescriptor fd) {
        interactionSocket.setInput(fd);
    }

    public OutputStream getOutputStream() {
        return interactionSocket.getOutputStream();
    }

    public String getToken() {
        return token;
    }

    public String getRouteInfo() {
        return routeInfo;
    }

    public String getSubProtocol() {
        return subProtocol;
    }

    public void close() {
        if (client != null) {
            try {
                client.stop();
            } catch (Exception e) {
            }
        }
        interactionSocket.close();
    }

    public boolean isClosed() {
        if (interactionSocket != null) {
            return interactionSocket.isClosed();
        } else {
            return true;
        }
    }
}
