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

package com.aliyun.odps.cupid.runtime;

import apsara.odps.cupid.protocol.PartitionSplitFileInfo;
import apsara.odps.cupid.protocol.ProxyAm;
import apsara.odps.cupid.protocol.SavePathsProtos;
import apsara.odps.cupid.protocol.StsTokenInfoProtos;
import com.google.protobuf.Service;

public abstract class RuntimeContext {
    public abstract Service getCupidClientService(String serviceName) throws Exception;
    public abstract void setCupidClientService(Service service) throws Exception;
    public abstract void stopChannelServer() throws Exception;
    public abstract void addRpcRequestHandler(RPCRequestHandleWrapper handler) throws Exception;
    public abstract RPCRequestHandleWrapper getRpcRequestHandler(String method) throws Exception;

    public abstract void reportApplicationMasterFinishStatus(int progress, String status) throws Exception;
    public abstract void reportContainerStatus(ContainerStatus status, String message) throws Exception;
    public TableWriterAbstract createTableWriterByLabel(String schema, String fileName, String label) throws Exception
    {
        return null;
    }


    public abstract void updateInputOutputInfos(InputOutputInfosManager inputOutputInfosManager) throws Exception;
    public abstract PartitionSplitFileInfo.PartitionSplitInfo generatePartitonInfo(String tableInputId, String splitTempDir) throws Exception;
    public abstract CreateTableReaderResult createTableReader(String tableInputId,
                               String partitionId,
                               String splitStart,
                               String splitEnd,
                               String schemaSplitStart,
                               String schemaSplitEnd,
                               String splitTempDir) throws Throwable;

    public abstract void commitTaskRenamePaths(SavePathsProtos.SavePaths saveResults, String taskAttemptId) throws Exception;
    public abstract String rpcRequestWrapper(byte[] parameter) throws Exception;
    public abstract StsTokenInfoProtos.GetStsTokenRes getStsToken(String roleArn, String stsTokenType) throws Exception;
    public abstract String getBearerToken() throws Exception;
    public abstract ProxyAm.ContainersFromPreviousAttemptAmLib getContainersFromPreviousAttemptAmLib() throws Exception;

    private static RuntimeContext context = null;
    public static void set(RuntimeContext other) {
        context = other;
    }

    public static RuntimeContext get()
    {
        return context;
    }
}
