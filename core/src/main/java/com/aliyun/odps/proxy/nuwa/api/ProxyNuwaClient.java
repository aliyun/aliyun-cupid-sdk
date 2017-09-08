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

package com.aliyun.odps.proxy.nuwa.api;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;

public abstract class ProxyNuwaClient {

    private static ProxyNuwaClient instance = null;

    protected ProxyNuwaClient() {
    }

    public static ProxyNuwaClient getInstance() {
        if (instance == null) {
            try {
                Class clz = Class.forName("com.aliyun.odps.proxy.nuwa.api.impl.ProxyNuwaClientImpl");
                Constructor<ProxyNuwaClient> meth =
                        (Constructor<ProxyNuwaClient>) clz.getDeclaredConstructor(new Class[]{});
                meth.setAccessible(true);
                instance = meth.newInstance();
            } catch (Exception e) {
                System.err.println("ProxyNuwaClient initialized failed: " + e.toString());
                e.printStackTrace(System.err);
            }
        }
        return instance;
    }

    public abstract void createFile(String path, byte[] data, boolean isEphemeral) throws IOException;

    public abstract void deleteFile(String path) throws IOException;

    public abstract void updateFile(String path, byte[] data) throws IOException;

    public abstract byte[] readFile(String path) throws IOException;

    public abstract boolean exists(String path) throws IOException;

    public abstract void subscribeFolder(String path, SubscribeCallback callBack) throws IOException;

    public abstract void unSubscribeFolder(String path) throws IOException;

    public abstract void subscribeFile(String path, SubscribeCallback callBack) throws IOException;

    public abstract void unSubscribeFile(String path) throws IOException;

    public abstract Map<String, byte[]> readFolder(String path) throws IOException;

    public abstract List<String> listFolder(String path) throws IOException;
}
