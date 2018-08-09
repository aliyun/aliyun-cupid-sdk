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

package com.aliyun.odps.cupid.table;

import java.io.Serializable;

public class TableOutputHandle implements Serializable {
    private static final long serialVersionUID = -6208226025179440180L;

    private String tableOutputHandleId;
    private String projectName;
    private String tableName;

    public TableOutputHandle(String tableOutputHandleId,
                             String projectName,
                             String tableName) {
        this.tableOutputHandleId = tableOutputHandleId;
        this.projectName = projectName;
        this.tableName = tableName;
    }

    public String getTableOutputHandleId() {
        return tableOutputHandleId;
    }

    public void setTableOutputHandleId(String tableOutputHandleId) {
        this.tableOutputHandleId = tableOutputHandleId;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
