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

import com.aliyun.odps.cupid.CupidSession;

import java.lang.reflect.Constructor;

public class TableImplUtils {

    public static String TableInputFormatImplKey = "odps.cupid.table.input.format.impl";
    public static String TableOutputFormatImplKey = "odps.cupid.table.output.format.impl";

    private static TableInputFormat tableInputFormatInstance = null;
    public static TableInputFormat getOrCreateInputFormat() throws Exception {
        if (tableInputFormatInstance == null)
        {
            tableInputFormatInstance = newInputFormatInstance();
        }
        return tableInputFormatInstance;
    }

    public static TableInputFormat newInputFormatInstance() throws Exception {
        String tableInputFormatImplClass =
                CupidSession.getConf().
                        get(TableInputFormatImplKey,
                                "com.aliyun.odps.cupid.table.impl.TableInputFormatImpl"
                        );
        try {
            Class clz = Class.forName(tableInputFormatImplClass);
            Constructor<TableInputFormat> meth = (Constructor<TableInputFormat>) clz.getDeclaredConstructor(new Class[]{});
            meth.setAccessible(true);
            return meth.newInstance();
        } catch (Exception e) {
            System.err.println("TableInputFormat initialized failed: " + e.toString());
            return null;
        }
    }

    private static TableOutputFormat tableOutputFormatInstance = null;
    public static TableOutputFormat getOrCreateOutputFormat() throws Exception {
        if (tableOutputFormatInstance == null)
        {
            tableOutputFormatInstance = newOutputFormatInstance();
        }
        return tableOutputFormatInstance;
    }

    public static TableOutputFormat newOutputFormatInstance() throws Exception {
        String tableOutputFormatImplClass =
                CupidSession.getConf().
                        get(TableOutputFormatImplKey,
                                "com.aliyun.odps.cupid.table.impl.TableOutputFormatImpl"
                        );
        try {
            Class clz = Class.forName(tableOutputFormatImplClass);
            Constructor<TableOutputFormat> meth = (Constructor<TableOutputFormat>) clz.getDeclaredConstructor(new Class[]{});
            meth.setAccessible(true);
            return meth.newInstance();
        } catch (Exception e) {
            System.err.println("TableOutputFormat initialized failed: " + e.toString());
            return null;
        }
    }
}
