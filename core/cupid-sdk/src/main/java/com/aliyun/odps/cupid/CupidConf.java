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

package com.aliyun.odps.cupid;


import com.aliyun.odps.cupid.utils.JTuple;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class CupidConf {
    private Logger logger = Logger.getLogger(CupidConf.class);
    private ConcurrentHashMap<String, String> settings;

    public CupidConf()
    {
        settings = new ConcurrentHashMap<String, String>();
    }


    public String get(String key)
    {
        if (settings.containsKey(key))
        {
            return settings.get(key);
        }
        else
        {
            throw new NoSuchElementException(key);
        }
    }

    public String get(String key, String defaultValue)
    {
        if (settings.containsKey(key))
        {
            return settings.get(key);
        }
        else
        {
            return defaultValue;
        }
    }

    public boolean getBoolean(String key, boolean defaultValue)
    {
        if (settings.containsKey(key))
        {
            return Boolean.parseBoolean(settings.get(key));
        }
        else
        {
            return defaultValue;
        }
    }

    public JTuple.JTuple2<String, String>[] getAll()
    {
        ArrayList<JTuple.JTuple2<String, String>> allConfs = new ArrayList<JTuple.JTuple2<String, String>>();
        for (Map.Entry<String, String> entry : settings.entrySet())
        {
            allConfs.add(JTuple.tuple(entry.getKey(), entry.getValue()));
        }

        return allConfs.toArray(new JTuple.JTuple2[allConfs.size()]);
    }

    public CupidConf remove(String key)
    {
        settings.remove(key);
        return this;
    }

    public CupidConf set(String key, String value)
    {
        if (key == null) {
            throw new NullPointerException("null key");
        }
        if (value == null) {
            throw new NullPointerException("null value for " + key);
        }

        settings.put(key.trim(), value.trim());
        return this;
    }
}
