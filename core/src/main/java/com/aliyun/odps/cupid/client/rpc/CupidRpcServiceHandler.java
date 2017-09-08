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

package com.aliyun.odps.cupid.client.rpc;

import com.google.protobuf.*;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class CupidRpcServiceHandler {
    private static final Logger LOG = Logger.getLogger(CupidRpcServiceHandler.class);

    private static CupidRpcServiceHandler INSTANCE = null;

    public static CupidRpcServiceHandler getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new CupidRpcServiceHandler();
        }
        return INSTANCE;
    }

    public class ServiceHandlerResponse {
        public RpcController getController() {
            return controller;
        }

        public byte[] getResponse() {
            return response;
        }

        private RpcController controller;
        private byte[] response;

        ServiceHandlerResponse(RpcController controller, byte[] response) {
            this.controller = controller;
            this.response = response;
        }

        public boolean isFailed() {
            return this.controller.failed();
        }

        public String getFailedMessage() {
            return this.controller.errorText();
        }
    }

    public ServiceHandlerResponse handle(Service service, int methodId, byte[] requestBody) {
        Descriptors.MethodDescriptor method = service.getDescriptorForType().getMethods().get(methodId);
        final ByteArrayOutputStream resOut = new ByteArrayOutputStream();
        final RpcController controller = new CupidClientRpcController();
        try {
            com.google.protobuf.Message reqMessage = null;
            reqMessage = service.getRequestPrototype(method).newBuilderForType().mergeFrom(requestBody).build();
            final StringBuffer sb = new StringBuffer();
            RpcCallback callback = new RpcCallback<com.google.protobuf.Message>() {
                public void run(com.google.protobuf.Message message) {
                    try {
                        message.writeTo(resOut);
                    } catch (IOException e) {
                        LOG.error("serialize protobuf rpc response fail", e);
                        sb.append("serialize protobuf rpc response fail: " + e);
                    }
                }
            };
            service.callMethod(method, controller, reqMessage, callback);
            if (!controller.failed() && sb.length() > 0) {
                controller.setFailed(sb.toString());
            }
        } catch (InvalidProtocolBufferException e) {
            controller.setFailed(e.toString());
        }
        return new ServiceHandlerResponse(controller, resOut.toByteArray());
    }
}
