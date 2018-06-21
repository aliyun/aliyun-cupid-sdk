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

package com.aliyun.odps.cupid.basic.rpc;

import apsara.odps.cupid.protocol.CupidTaskParamProtos;
import com.aliyun.odps.cupid.CupidSession;
import com.aliyun.odps.cupid.requestcupid.TaskServiceRequestUtil;
import com.google.protobuf.*;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CupidTaskServiceRpcChannel implements RpcChannel
{
    private static final Logger LOG = LoggerFactory.getLogger(CupidTaskServiceRpcChannel.class);
    private CupidSession cupidSession = null;

    public CupidTaskServiceRpcChannel(CupidSession cupidSession)
    {
        this.cupidSession = cupidSession;
    }

    public void callMethod(
            Descriptors.MethodDescriptor method,
            RpcController controller,
            Message request,
            Message responsePrototype,
            RpcCallback<Message> done)
    {
        try
        {
            CupidTaskParamProtos.TaskServiceRequest.Builder taskServiceReq =
                    CupidTaskParamProtos.TaskServiceRequest.newBuilder();
            taskServiceReq.setMethodName(method.getName());
            taskServiceReq.setRequestInBytes(request.toByteString());

            String res = TaskServiceRequestUtil.sendTaskServiceRequest(taskServiceReq, cupidSession);
            Message resMessage =
                    responsePrototype.newBuilderForType().mergeFrom(Base64.decodeBase64(res.getBytes())).build();
            done.run(resMessage);
        }
        catch (Exception ex)
        {
            controller.setFailed(ex.toString());
        }
    }
}
