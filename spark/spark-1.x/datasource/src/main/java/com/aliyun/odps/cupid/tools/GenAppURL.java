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

package com.aliyun.odps.cupid.tools;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Instances;
import com.aliyun.odps.Odps;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.request.cupid.CupidRequestProxy;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Created by liwei.li on 12/18/16.
 */
public class GenAppURL {
    private Map<String, String> confs;

    public GenAppURL(String confPath) throws IOException {
        File file = new File(confPath);
        if (!file.exists()) {
            throw new IOException("Conf file does not exist");
        }
        try (InputStreamReader inReader = new InputStreamReader(new FileInputStream(file), "UTF-8")) {
            confs = new HashMap<>();
            Properties properties = new Properties();
            properties.load(inReader);
            for (String key : properties.stringPropertyNames()) {
                String value = properties.getProperty(key).trim();
                confs.put(key.trim(), value);
            }
        } catch (IOException e) {
            throw new IOException("Failed when loading Spark properties from $filename", e);
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Please add the args: confpath [instanceid]");
            System.exit(0);
        }
        String confPath = args[0];
        String instanceId = args.length > 1 ? args[1] : null;

        GenAppURL tools = new GenAppURL(confPath);

        if (instanceId == null) {
            Map<String, String> kv = tools.getRunningInstances();
            for (Map.Entry<String, String> entry : kv.entrySet()) {
                if (null != entry.getValue()) {
                    System.out.println(entry.getKey() + "\n" + entry.getValue());
                }
            }
        } else {
            System.out.println(instanceId + "\n" + tools.getJobViewUrl(instanceId.trim()));
        }
    }

    public Odps getOdps() {
        AliyunAccount account = new AliyunAccount(confs.get("access_id"), confs.get("access_key"));
        Odps odps = new Odps(account);
        odps.setEndpoint(confs.get("end_point"));
        odps.setDefaultProject(confs.get("project_name"));
        return odps;
    }

    private Map<String, String> getRunningInstances() {
        Instances instances = getOdps().instances();
        if (instances == null) {
            return new HashMap<>();
        }

        Map<String, String> map = new HashMap<>();
        while (instances.iterator().hasNext()) {
            Instance instance = instances.iterator().next();
            if (instance.getStatus() == Instance.Status.RUNNING) {
                try {
                    String jobViewUrl = getJobViewUrl(instance.getId());
                    map.put(instance.getId(), jobViewUrl);
                } catch (IOException e) {
                    System.err.println("Fail to get jobview url for " + instance.getId());
                }
            }
        }
        return map;
    }

    public String getJobViewUrl(String instanceId) throws IOException {
        return CupidRequestProxy.getInstance().cupidRequestRPC(instanceId, getOdps(), instanceId, "requestappurlfrommeta");
    }
}