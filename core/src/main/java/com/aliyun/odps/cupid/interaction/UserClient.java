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

package com.aliyun.odps.cupid.interaction;

import com.aliyun.odps.cupid.utils.SDKConstants;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

public class UserClient {

    private final static int EOF = -1;

    private String endpoint;

    private String token;

    private String routeInfo;

    private InteractionClient client;

    private long defaultTimeout = 10 * 1000;

    private long defaultWaitAppTimeout = 5 * 60 * 1000;

    private Object startLock = new Object();

    private boolean isAppStart = false;

    public UserClient(String endpoint) {
        this.endpoint = endpoint;
    }

    public void connect() {
        connect(defaultTimeout);
    }


    public void connect(long timeout) {
        if (client == null || client.isClosed()) {
            client = InteractionClientFactory.getUserClient(Protocol.WEBSOCKET.name(), endpoint);
        } else {
            throw new RuntimeException("The client is connected!");
        }
        token = client.getToken();
        routeInfo = client.getRouteInfo();
        // wait to connect
        long endTime = System.currentTimeMillis() + timeout;
        while (client.isClosed()) {
            if (System.currentTimeMillis() > endTime) {
                throw new RuntimeException("Connect timeout! Check your endpoint and networks~");
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }
        }

        // connected
        new Thread(new InputWorker(client.getInputStream())).start();
        new Thread(new OutputWorker(client.getOutputStream())).start();
    }

    public String getToken() {
        return token;
    }

    public String getRouteInfo() {
        return routeInfo;
    }

    public void startImmediately() {
        synchronized (startLock) {
            isAppStart = true;
            startLock.notify();
        }
    }

    class InputWorker implements Runnable {

        private InputStream in;
        private boolean first = true;
        private int result = 1;

        InputWorker(InputStream in) {
            this.in = in;
        }

        public void run() {
            int b;
            try {
                while ((b = in.read()) != EOF) {
                    if (first) {
                        try {
                            Thread.sleep(8000);
                        } catch (InterruptedException e) {
                        }
                        first = false;
                    }
                    System.out.write(b & 0xff);
                    System.out.flush();
                }
                result = 0;
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (in != null)
                        in.close();
                } catch (IOException e) {
                }
                System.out.println();
                System.out.println("Bye!");
                System.exit(result);
            }
        }

    }

    class OutputWorker implements Runnable {

        private OutputStream out;

        private int result = 1;

        OutputWorker(OutputStream out) {
            this.out = out;
        }

        public void run() {

            synchronized (startLock) {
                if (!isAppStart) {
                    try {
                        startLock.wait(defaultWaitAppTimeout);
                    } catch (InterruptedException e) {
                    }
                }
            }

            try {
                ConsoleReader reader =
                        new ConsoleReader(System.in, new OutputStreamWriter(out, SDKConstants.DEFAULT_CHARSET));
                while (reader.readLine() != null) {
                }
                result = 0;
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (out != null)
                        out.close();
                } catch (IOException e) {
                }
                System.out.println();
                System.out.println("Bye!");
                System.exit(result);
            }
        }
    }
}
