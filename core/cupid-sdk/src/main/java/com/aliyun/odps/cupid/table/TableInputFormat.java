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

public abstract class TableInputFormat {

    /**
     * Via CupidTask API, Please make sure CupidSession.get not null.
     * Do input split according to the input tables.
     * use splitSize + splitCount to determine how many splits will be generated.
     * @param tables
     * @param splitSize
     * @param splitCount
     * @return TableInputHandle will be returned for further use, fetch splits info and read split content.
     * @throws Exception
     */
    public abstract TableInputHandle splitTables(TableInputInfo[] tables, int splitSize, int splitCount) throws Exception;

    /**
     * Via SubProcess API.
     * Fetch splits info, which contains the parameters on how to reading the target CFile.
     * @param handle TableInputHandle.
     * @return Table splits info, which could be use for further reading action.
     * @throws Exception
     */
    public abstract InputSplit[] getSplits(TableInputHandle handle) throws Exception;

    /**
     * Via SubProcess API.
     * Initialize a TableReader include schema and iterator that could be used for reading Records.
     * @param handle
     * @param split
     * @return TableReader which contains schema info and a record iterator.
     * @throws Throwable
     */
    public abstract TableReader readSplit(TableInputHandle handle, InputSplit split) throws Throwable;
}
