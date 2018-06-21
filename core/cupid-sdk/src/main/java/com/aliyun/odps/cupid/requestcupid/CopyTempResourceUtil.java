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

package com.aliyun.odps.cupid.requestcupid;

import apsara.odps.cupid.protocol.CupidTaskParamProtos.CupidTaskParam;
import apsara.odps.cupid.protocol.CupidTaskParamProtos.LocalResourceType;
import apsara.odps.cupid.protocol.CupidTaskParamProtos.OdpsLocalResource;
import apsara.odps.cupid.protocol.CupidTaskParamProtos.OdpsLocalResourceItem;
import com.aliyun.odps.FileResource;
import com.aliyun.odps.Odps;
import com.aliyun.odps.cupid.CupidConf;
import com.aliyun.odps.cupid.CupidSession;
import com.aliyun.odps.cupid.utils.JTuple;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;

public class CopyTempResourceUtil {

    private static Logger logger = Logger.getLogger(CopyTempResourceUtil.class);

    private static String[] copyTempResourceToFuxiJobDir(CupidSession session, String[] tmpResources) throws Exception {
        CupidTaskParam.Builder cupidTaskParamBuilder =
                CupidTaskBaseUtil.getOperationBaseInfo(
                        session,
                        CupidTaskOperatorConst.CUPID_TASK_COPY_TEMPRESOURCE,
                        session.getJobLookupName(),
                        null
                );

        OdpsLocalResource.Builder odpsLocalResourceBuilder = OdpsLocalResource.newBuilder();
        OdpsLocalResourceItem.Builder odpsLocalResourceBuilderItemBuilder = OdpsLocalResourceItem.newBuilder();
        ArrayList<String> ret = new ArrayList<String>();
        for (String res : tmpResources)
        {
            ret.add(res);
            odpsLocalResourceBuilderItemBuilder.setProjectname(session.conf.get("odps.project.name"));
            odpsLocalResourceBuilderItemBuilder.setRelativefilepath(res);
            odpsLocalResourceBuilderItemBuilder.setType(LocalResourceType.TempResource);
            odpsLocalResourceBuilder.addLocalresourceitem(odpsLocalResourceBuilderItemBuilder.build());
        }
        cupidTaskParamBuilder.setLocalresource(odpsLocalResourceBuilder.build());
        SubmitJobUtil.copyTempResourceSubmitJob(session, cupidTaskParamBuilder.build());
        return ret.toArray(new String[ret.size()]);
    }

    public static void addTempResource(String tmpResName, String file, Odps odps) throws Throwable {
        addTempResource(CupidSession.get(), tmpResName, file, odps);
    }

    public static void addTempResource(CupidSession session, String tmpResName, String file, Odps odps)
            throws Throwable {
        FileResource res = new FileResource();
        res.setName(tmpResName);
        res.setIsTempResource(true);
        logger.info("begin creating tempResource: " + tmpResName);
        CupidConf conf = session.conf;
        int retryCount = Integer.parseInt(conf.get("spark.copyFileToRemote.retry.count", "3"));
        while (retryCount > 0) {
            FileInputStream in = new FileInputStream(file);
            try
            {
                if (odps.resources().exists(odps.getDefaultProject(), tmpResName))
                {
                    logger.info("tempResource existed: " + tmpResName);
                }
                else
                {
                    odps.resources().create(odps.getDefaultProject(), res, in);
                    logger.info("create tempResource " + tmpResName + " success");
                }
                retryCount = 0;
            }
            catch (Throwable e)
            {
                logger.warn("create tempResource " + tmpResName + " fail", e);
                retryCount -= 1;
                if (retryCount == 0) {
                    throw e;
                }
            } finally {
                in.close();
            }
        }
    }

    public static JTuple.JTuple2<String, String> addAndCopyTempResource(String srcPath, Odps odps) throws Throwable {
      return addAndCopyTempResource(CupidSession.get(), srcPath, odps);
    }

    /*
     * add file to temp resource and copy it from control cluster to computing cluster
     * @param srcPath local file path
     * @return the relative path to the fuxi job temp dir.
     */
    public static JTuple.JTuple2<String, String> addAndCopyTempResource(CupidSession session, String srcPath, Odps odps)
            throws Throwable {
        File file = new File(srcPath);
        FileInputStream in0 = new FileInputStream(file);
        String md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(in0);
        in0.close();
        String tmpResName = md5 + "_" + file.getName();
        addTempResource(session, tmpResName, file.getAbsolutePath(), odps);
        String[] resources = {
            tmpResName
        };
        return JTuple.tuple(tmpResName, copyTempResourceToFuxiJobDir(session, resources)[0]);
    }
}
