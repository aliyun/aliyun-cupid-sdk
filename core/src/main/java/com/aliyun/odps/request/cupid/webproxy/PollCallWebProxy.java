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

import org.apache.log4j.Logger;

public class PollCallWebProxy {
    private static final Logger LOG = Logger.getLogger(WebProxyCall.class);
    private static final PollCallWebProxy INSTANCE = new PollCallWebProxy();

    public static PollCallWebProxy getInstance() {
        return INSTANCE;
    }


    class CallThread extends Thread {
        private String callUrl;
        private int callInterval;

        CallThread(String url, int callInterval) {
            this.callUrl = url;
            this.callInterval = callInterval;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    WebProxyCall.getInstance().callWebProxy(callUrl);
                    Thread.sleep(callInterval * 1000);
                } catch (InterruptedException e) {
                    LOG.warn("in call webproxy " + e.getMessage());
                }
            }
        }
    }

    public void startPoll(String url, int callInterval) {
        CallThread callThread = new CallThread(url, callInterval);
        callThread.setDaemon(true);
        callThread.start();
    }
}
