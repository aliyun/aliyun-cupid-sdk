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

import apsara.odps.cupid.client.protocol.CupidClientRpc;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CupidRpcChannelProxyTest extends CupidRpcChannelProxy {
    private static final Logger LOG = LoggerFactory
            .getLogger(CupidClientRpcChannel.class);
    private Service service;

    public CupidRpcChannelProxyTest(Service service) {
        this.service = service;
    }

    @Override
    public byte[] SyncCall(byte[] res) {
        CupidClientRpc.RpcProtocol rpcProtocol = null;
        try {
            rpcProtocol = CupidClientRpc.RpcProtocol.parseFrom(res);
            int methodId = rpcProtocol.getMethodId();
            byte[] requestBody = rpcProtocol.getRequestBody().toByteArray();
            CupidRpcServiceHandler.ServiceHandlerResponse serviceHandlerResponse = CupidRpcServiceHandler.getInstance().handle(service, methodId, requestBody);
            if (serviceHandlerResponse.isFailed()) {
                LOG.error("The rpc call error:" + serviceHandlerResponse.getFailedMessage());
            }
            return serviceHandlerResponse.getResponse();
        } catch (InvalidProtocolBufferException e) {
            LOG.error("The rpc call error:" + e.getMessage());
            return null;
        }
    }
}
