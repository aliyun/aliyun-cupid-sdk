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

package org.apache.spark.streaming.loghub;

import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition;
import org.apache.spark.storage.StorageLevel;

import java.io.Serializable;

/**
 * Loghub Input Parameter
 *
 * User: 六翁 lu.hl@alibaba-inc.com
 * Date: 2017-01-16
 */
public class StreamingParam implements Serializable {
    /*spark.logservice.fetch.inOrder*/
    private boolean inOrder;
    /*spark.logservice.heartbeat.interval.millis*/
    private long hbInterval;
    /*spark.logservice.fetch.interval.millis*/
    private long fetchInterval;
    /*窗口期间隔毫秒数*/
    private long batchInterval;
    /*Loghub项目名称*/
    private String project;
    /*Loghub日志库名称*/
    private String logstore;
    /*Loghub协同消费分组*/
    private String group;
    /*任务实例名称，不用设置*/
    private String instance;
    /*Loghub地址*/
    private String endpoint;
    /*阿里云访问密钥AccessKeyId*/
    private String id;
    /*阿里云访问密钥AccessKeySecret*/
    private String secret;
    /*默认StorageLevel.MEMORY_AND_DISK*/
    /*默认LogHubCursorPosition.END_CURSOR*/
    private LogHubCursorPosition cursor;
    private StorageLevel level;
    /*如果cursor设置为LogHubCursorPosition.SPECIAL_TIMER_CURSOR 指定的流开始的时间戳 默认-1*/
    private int start;
    /*强制LogHubCursorPosition.SPECIAL_TIMER_CURSOR 默认为false*/
    private boolean forceSpecial;

    public boolean isInOrder() {
        return inOrder;
    }

    public long getHbInterval() {
        return hbInterval;
    }

    public long getFetchInterval() {
        return fetchInterval;
    }

    public long getBatchInterval() {
        return batchInterval;
    }

    public String getProject() {
        return project;
    }

    public String getLogstore() {
        return logstore;
    }

    public String getGroup() {
        return group;
    }

    public String getInstance() {
        return instance;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getId() {
        return id;
    }

    public String getSecret() {
        return secret;
    }

    public StorageLevel getLevel() {
        return level;
    }

    public LogHubCursorPosition getCursor() {
        return cursor;
    }

    public int getStart() {
        return start;
    }

    public boolean isForceSpecial() {
        return forceSpecial;
    }

    public void setInOrder(boolean inOrder) {
        this.inOrder = inOrder;
    }

    public void setHbInterval(long hbInterval) {
        this.hbInterval = hbInterval;
    }

    public void setFetchInterval(long fetchInterval) {
        this.fetchInterval = fetchInterval;
    }

    public void setBatchInterval(long batchInterval) {
        this.batchInterval = batchInterval;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public void setLogstore(String logstore) {
        this.logstore = logstore;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setSecret(String secret) {
        this.secret = secret;
    }

    public void setLevel(StorageLevel level) {
        this.level = level;
    }

    public void setCursor(LogHubCursorPosition cursor) {
        this.cursor = cursor;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public void setForceSpecial(boolean forceSpecial) {
        this.forceSpecial = forceSpecial;
    }
}
