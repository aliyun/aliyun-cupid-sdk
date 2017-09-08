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

import com.aliyun.odps.Odps;
import com.aliyun.odps.request.cupid.CupidRequestProxy;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;

public class CupidHttpRpcChannelProxy extends CupidRpcChannelProxy {
    private Odps odps = null;
    private String lookupName = null;

    public CupidHttpRpcChannelProxy(Odps odps, String lookupName) {
        this.odps = odps;
        this.lookupName = lookupName;
    }

    @Override
    public byte[] SyncCall(byte[] res) throws IOException {
        String rpcData = Base64.encodeBase64String(res);
        String requestResponse = CupidRequestProxy.getInstance().cupidRequestRPC(rpcData, odps, lookupName);
        return Base64.decodeBase64(requestResponse);
    }
}
