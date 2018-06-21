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

package com.aliyun.odps.cupid.clusterkv;

import apsara.odps.cupid.protocol.CupidTaskServiceProto.*;
import com.aliyun.odps.cupid.CupidSession;
import com.aliyun.odps.cupid.basic.rpc.CupidTaskServiceRpcChannel;
import com.aliyun.odps.cupid.basic.rpc.CupidTaskServiceRpcController;
import com.google.protobuf.RpcCallback;

import java.util.HashMap;
import java.util.Map;

/**
 * @author linxuewei
 */
public class ClusterKvManager
{
    private volatile static ClusterKvManager instance;
    private ClusterKvManager() {}
    public static ClusterKvManager getInstance() {
        if (instance == null) {
            synchronized (ClusterKvManager.class) {
                if (instance == null) {
                    instance = new ClusterKvManager();
                }
            }
        }
        return instance;
    }

    public void putOrUpdate(CupidSession cupidSession, String key, String value) throws Exception
    {
        CupidTaskServiceRpcController controller = new CupidTaskServiceRpcController();
        CupidTaskServiceRpcChannel channel = new CupidTaskServiceRpcChannel(cupidSession);
        CupidTaskService.Stub stub = CupidTaskService.newStub(channel);

        PutOrCreateClusterKvRequest.Builder request = PutOrCreateClusterKvRequest.newBuilder();
        request.setProjectName(cupidSession.odps().getDefaultProject());
        request.setKey(key);
        request.setValue(value);

        final PutOrCreateClusterKvResponse.Builder response = PutOrCreateClusterKvResponse.newBuilder();
        RpcCallback<PutOrCreateClusterKvResponse> callback =
                new RpcCallback<PutOrCreateClusterKvResponse>() {
                    @Override
                    public void run(PutOrCreateClusterKvResponse parameter) {
                        response.mergeFrom(parameter);
                    }
                };

        stub.putOrCreateClusterKv(controller, request.build(), callback);
        if (controller.failed()) {
            throw new Exception(controller.errorText());
        }
    }

    public void delete(CupidSession cupidSession, String key) throws Exception
    {
        CupidTaskServiceRpcController controller = new CupidTaskServiceRpcController();
        CupidTaskServiceRpcChannel channel = new CupidTaskServiceRpcChannel(cupidSession);
        CupidTaskService.Stub stub = CupidTaskService.newStub(channel);

        DeleteClusterKvRequest.Builder request = DeleteClusterKvRequest.newBuilder();
        request.setProjectName(cupidSession.odps().getDefaultProject());
        request.setKey(key);

        final DeleteClusterKvResponse.Builder response = DeleteClusterKvResponse.newBuilder();
        RpcCallback<DeleteClusterKvResponse> callback =
                new RpcCallback<DeleteClusterKvResponse>() {
                    @Override
                    public void run(DeleteClusterKvResponse parameter) {
                        response.mergeFrom(parameter);
                    }
                };

        stub.deleteClusterKv(controller, request.build(), callback);
        if (controller.failed()) {
            throw new Exception(controller.errorText());
        }
    }

    public String get(CupidSession cupidSession, String key) throws Exception
    {
        CupidTaskServiceRpcController controller = new CupidTaskServiceRpcController();
        CupidTaskServiceRpcChannel channel = new CupidTaskServiceRpcChannel(cupidSession);
        CupidTaskService.Stub stub = CupidTaskService.newStub(channel);

        GetClusterKvRequest.Builder request = GetClusterKvRequest.newBuilder();
        request.setProjectName(cupidSession.odps().getDefaultProject());
        request.setKey(key);

        final GetClusterKvResponse.Builder response = GetClusterKvResponse.newBuilder();
        RpcCallback<GetClusterKvResponse> callback =
                new RpcCallback<GetClusterKvResponse>() {
                    @Override
                    public void run(GetClusterKvResponse parameter) {
                        response.mergeFrom(parameter);
                    }
                };

        stub.getClusterKv(controller, request.build(), callback);
        if (controller.failed()) {
            throw new Exception(controller.errorText());
        }

        return response.getValue();
    }

    public Map<String, String> listByPrefix(CupidSession cupidSession) throws Exception
    {
        return listByPrefix(cupidSession, null);
    }

    public Map<String, String> listByPrefix(CupidSession cupidSession, String prefix) throws Exception
    {
        CupidTaskServiceRpcController controller = new CupidTaskServiceRpcController();
        CupidTaskServiceRpcChannel channel = new CupidTaskServiceRpcChannel(cupidSession);
        CupidTaskService.Stub stub = CupidTaskService.newStub(channel);

        ListByPrefixClusterKvRequest.Builder request = ListByPrefixClusterKvRequest.newBuilder();
        request.setProjectName(cupidSession.odps().getDefaultProject());
        if (prefix != null)
        {
            request.setPrefix(prefix);
        }

        final ListByPrefixClusterKvResponse.Builder response = ListByPrefixClusterKvResponse.newBuilder();
        RpcCallback<ListByPrefixClusterKvResponse> callback =
                new RpcCallback<ListByPrefixClusterKvResponse>() {
                    @Override
                    public void run(ListByPrefixClusterKvResponse parameter) {
                        response.mergeFrom(parameter);
                    }
                };

        stub.listByPrefixClusterKv(controller, request.build(), callback);
        if (controller.failed()) {
            throw new Exception(controller.errorText());
        }

        Map<String, String> clusterConf = new HashMap<String, String>();
        for (ClusterKv clusterKv : response.getClusterKvList())
        {
            clusterConf.put(clusterKv.getKey(), clusterKv.getValue());
        }

        return clusterConf;
    }
}
