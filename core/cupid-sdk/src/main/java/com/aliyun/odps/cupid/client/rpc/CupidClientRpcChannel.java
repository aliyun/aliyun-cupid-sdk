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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import apsara.odps.cupid.client.protocol.CupidClientRpc;

import java.io.IOException;

public class CupidClientRpcChannel implements RpcChannel {
    private static final Logger LOG = LoggerFactory
            .getLogger(CupidClientRpcChannel.class);

    CupidRpcChannelProxy cupidRpcChannelProxy = null;

    public CupidClientRpcChannel(CupidRpcChannelProxy cupidRpcChannelProxy){
        this.cupidRpcChannelProxy = cupidRpcChannelProxy;
    }

    public void callMethod(Descriptors.MethodDescriptor method,
                           RpcController controller,
                           Message request,
                           Message responsePrototype,
                           RpcCallback<Message> done) {
        try {
            int index = method.getIndex();
            CupidClientRpc.RpcProtocol.Builder rpcProtocolBuilder = CupidClientRpc.RpcProtocol.newBuilder();
            rpcProtocolBuilder.setServiceName(method.getService().getFullName());
            rpcProtocolBuilder.setMethodId(index);
            rpcProtocolBuilder.setRequestBody(request.toByteString());

            byte[] res = this.cupidRpcChannelProxy.SyncCall(rpcProtocolBuilder.build().toByteArray());
            Message resMessage = responsePrototype.newBuilderForType().mergeFrom(res).build();
            done.run(resMessage);
        } catch (IOException e) {
            controller.setFailed(e.toString());
        }

    }
}