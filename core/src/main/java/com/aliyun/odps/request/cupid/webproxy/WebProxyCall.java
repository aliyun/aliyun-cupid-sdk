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

package com.aliyun.odps.request.cupid.webproxy;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

public class WebProxyCall {
    private static final Logger LOG = Logger.getLogger(WebProxyCall.class);
    private static WebProxyCall INSTANCE = new WebProxyCall();

    public static WebProxyCall getInstance() {
        return INSTANCE;
    }

    public void callWebProxy(String url) {
        String resultCode = "";
        DefaultHttpClient httpclient = new DefaultHttpClient();
        try {
            HttpGet httpget = new HttpGet(url);
            HttpResponse response = httpclient.execute(httpget);

            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == HttpStatus.SC_OK) {
                HttpEntity entity = response.getEntity();
                resultCode = ResponseCode.CALLRESPONSEERROR;
                if (entity != null) {
                    String responseString = EntityUtils.toString(entity);
                    if (responseString.contains("Spark Jobs") && responseString.contains("Stages")
                            && responseString.contains("Storage") && responseString.contains("Environment")
                            && responseString.contains("Executors")) {
                        resultCode = ResponseCode.CALLSUCCESS;
                    }
                }
            } else if (statusCode == HttpStatus.SC_MOVED_TEMPORARILY
                    || statusCode == HttpStatus.SC_MOVED_PERMANENTLY) {
                resultCode = ResponseCode.CALLFORBIDDEN;
            } else {
                resultCode = ResponseCode.OTHER_RESPONSE + String.valueOf(statusCode);
            }
        } catch (Exception e) {
            LOG.warn("WebProxyCall exception " + e.getMessage());
            resultCode = ResponseCode.CALLEXCEPTION;
        } finally {
            httpclient.close();
        }
        LOG.info("WebProxyCall result " + resultCode);
        if (!resultCode.equals(ResponseCode.CALLSUCCESS)) {
            System.exit(1);
        }
    }
}
