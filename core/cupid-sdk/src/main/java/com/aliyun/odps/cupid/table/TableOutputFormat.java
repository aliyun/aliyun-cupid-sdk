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

import com.aliyun.odps.cupid.runtime.TableWriterAbstract;

public abstract class TableOutputFormat {

    /**
     * Via CupidTask API, Please make sure CupidSession.get not null.
     * Create a TableOutputHandle for projectName + tableName.
     * Will create/prepare ENV/settings/Cap for writing table.
     * @param projectName
     * @param tableName
     * @return TableOutputHandle will be returned for further use, get writer/commit file/commit table.
     * @throws Exception
     */
    public abstract TableOutputHandle writeTable(String projectName, String tableName) throws Exception;

    /**
     * Via SubProcess API.
     * Get writer to write record into attemptFileName.
     * A partSpec + A attemptFileName determined a Single tmp file.
     * @param handle
     * @param partSpec null or "" mean this table is non-partition table
     * @param attemptFileName
     * @return
     * @throws Exception
     */
    public abstract TableWriterAbstract writeTableFile(TableOutputHandle handle, String partSpec, String attemptFileName) throws Exception;

    /**
     * Via SubProcess API.
     * Move[Rename] the close tmp file into another directory that ready to DO ddl task.
     * A partSpec + A commitFileName determined a Single ddl file.
     * @param handle
     * @param commitFileEntries contains partSpec,attemptFileName,commitFileName
     * @throws Exception
     */
    public abstract void commitTableFiles(TableOutputHandle handle, CommitFileEntry[] commitFileEntries) throws Exception;

    /**
     * Via CupidTask API, Please make sure CupidSession.get not null.
     * Do external DDL for projectName + tableName + partSpecs.
     * !!! User need to provide the target partSpecs
     * @param handle
     * @param isOverWrite
     * @param partSpecs partSpecs should either be null, which means non-partition table; or be a string array contains multiple partSpec
     * @throws Exception
     */
    public abstract void commitTable(TableOutputHandle handle, boolean isOverWrite, String[] partSpecs) throws Exception;

    /**
     * Via CupidTask API, Please make sure CupidSession.get not null.
     * Do write table env clean up, mostly remove tmp data from pangu
     * @param handle
     * @throws Exception
     */
    public abstract void closeOutputHandle(TableOutputHandle handle) throws Exception;
}
