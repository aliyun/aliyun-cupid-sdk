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

package com.aliyun.odps.request.cupid;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.transport.Request;
import com.aliyun.odps.rest.JAXBUtils;
import com.aliyun.odps.rest.ResourceBuilder;

import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CupidRequestProxy {
    private final static CupidRequestProxy INSTANCE = new CupidRequestProxy();

    public static CupidRequestProxy getInstance() {
        return INSTANCE;
    }

    static class CupidRequestBase {
        @XmlElement(name = "RequestType")
        protected String requestType;
    }

    @XmlRootElement(name = "CupidRequestRPC")
    static class CupidRequestRPC extends CupidRequestBase {
        @XmlElement(name = "Data")
        private String data;
    }

    @XmlRootElement(name = "Item")
    static class ResponseForRequest {
        @XmlElement(name = "Data")
        private String data;
    }

    private ResponseForRequest callOdpsWorker(String bodyXmlStr, String httpUriType, Odps odps, String lookupName) throws IOException {
        // header, param
        Map<String, String> headers = new HashMap<String, String>();
        Map<String, String> params = new HashMap<String, String>();

        headers.put("Content-Type", "application/xml");
        params.put("type", httpUriType);

        StringBuilder sb = new StringBuilder();
        sb.append(ResourceBuilder.PROJECTS).append('/').append(ResourceBuilder.encodeObjectName(odps.getDefaultProject()));
        sb.append(ResourceBuilder.INSTANCES).append('/').append(ResourceBuilder.encodeObjectName(lookupName));
        sb.append("/rpccall");
        String resource = sb.toString();

        ResponseForRequest responseForRpc;
        try {
            responseForRpc = odps.getRestClient()
                    .stringRequest(ResponseForRequest.class, resource, Request.Method.POST.name(), params, headers, bodyXmlStr);
        } catch (OdpsException e) {
            throw new IOException(e.getMessage(), e);
        }
        return responseForRpc;
    }

    public String cupidRequestRPC(String rpcData, Odps odps, String lookupName) throws IOException {
        return this.cupidRequestRPC(rpcData, odps, lookupName, CupidRequestType.REQUESTRPC);
    }


    public String cupidRequestRPC(String rpcData, Odps odps, String lookupName, String requestType) throws IOException {
        String bodyXml;
        CupidRequestRPC cupidRequestRPC = new CupidRequestRPC();
        cupidRequestRPC.requestType = requestType;
        cupidRequestRPC.data = rpcData;
        try {
            bodyXml = JAXBUtils.marshal(cupidRequestRPC, CupidRequestRPC.class);
        } catch (JAXBException e) {
            throw new IOException(e);
        }
        ResponseForRequest response = callOdpsWorker(bodyXml, "cupid", odps, lookupName);
        return response.data;
    }
}
