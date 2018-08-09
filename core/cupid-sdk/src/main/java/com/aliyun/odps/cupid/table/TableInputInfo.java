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
import java.util.ArrayList;

/**
 * project + table + partSpec consider as a single SplitInput, if passing multiple TableInputInfo
 * make sure columns are the same
 */
public class TableInputInfo implements Serializable
{
    private static final long serialVersionUID = 7934303490785687617L;
    private String projectName;
    private String tableName;
    private String partSpec = null;
    private String[] columns;

    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
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

    public String getPartSpec() {
        return partSpec;
    }

    public void setPartSpec(String partSpec) {
        this.partSpec = partSpec;
    }

    /**
     * Default constructor
     * projectName tableName partSpec columns COULD be set later by setters
     */
    public TableInputInfo() {
        projectName = null;
        tableName = null;
        partSpec = null;
        columns = null;
    }

    /**
     * Constructor for normal table
     *
     * @param projectName
     * @param tableName
     * @param columns
     */
    public TableInputInfo(String projectName, String tableName, String[] columns) {
        this(projectName, tableName, columns, null);
    }

    /**
     * Constructor for partition table for single partition definition
     *
     * @param projectName
     * @param tableName
     * @param columns
     * @param partSpec    "pt1='p1',pt2='p2'"
     */
    public TableInputInfo(String projectName, String tableName, String[] columns, String partSpec) {
        this.projectName = projectName;
        this.tableName = tableName;
        this.columns = columns;
        this.partSpec = partSpec;
    }

    /**
     * generate TableInputInfo[] for multiple partSpec
     *
     * @param projectName
     * @param tableName
     * @param columns
     * @param partSpecs
     * @return
     */
    public static TableInputInfo[] generateTableInputInfos(
            String projectName,
            String tableName,
            String[] columns,
            String[] partSpecs
    ) throws Exception {
        ArrayList<TableInputInfo> tableInputInfos = new ArrayList<TableInputInfo>();
        if (partSpecs.length == 0) {
            tableInputInfos.add(
                    new TableInputInfo(projectName,
                            tableName,
                            columns,
                            null)
            );
        } else {
            for (String partSpec : partSpecs) {
                tableInputInfos.add(
                        new TableInputInfo(projectName,
                                tableName,
                                columns,
                                partSpec)
                );
            }
        }

        return tableInputInfos.toArray(new TableInputInfo[tableInputInfos.size()]);
    }
}
