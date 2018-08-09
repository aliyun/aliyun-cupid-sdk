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

package com.aliyun.odps.request.cupid;

import com.aliyun.odps.cupid.CupidConf;
import com.aliyun.odps.cupid.CupidSession;
import com.aliyun.odps.cupid.clusterkv.ClusterKvManager;
import org.junit.Assert;

import java.util.HashMap;
import java.util.Map;

public class ClusterKvManagerTest
{
    public static void main(String[] args) throws Exception
    {
        String k1 = "linxuewei.cupid.k1";
        String v1 = "linxuewei.cupid.v1";
        String k2 = "linxuewei.moye.k2";
        String v2 = "linxuewei.moye.v2";
        String k3 = "leanken.cupid.k1";
        String v3 = "leanken.cupid.v1";

        String k4_not_exist = "linxuewei.not.exist";

        CupidConf cupidConf = new CupidConf();
        cupidConf.set("odps.project.name", "test_copy");
        cupidConf.set("odps.end.point", "http://10.101.214.143:8577/api");
        cupidConf.set("odps.access.id", "63wd3dpztlmb5ocdkj94pxmm");
        cupidConf.set("odps.access.key", "");

        CupidSession cupidSession = new CupidSession(cupidConf);

        // put key
        ClusterKvManager.getInstance().putOrUpdate(cupidSession, k1, v1);
        ClusterKvManager.getInstance().putOrUpdate(cupidSession, k2, v2);
        ClusterKvManager.getInstance().putOrUpdate(cupidSession, k3, v3);

        // get test
        String response = ClusterKvManager.getInstance().get(cupidSession, k1);
        Assert.assertEquals(v1, response);
        response = ClusterKvManager.getInstance().get(cupidSession, k2);
        Assert.assertEquals(v2, response);
        response = ClusterKvManager.getInstance().get(cupidSession, k3);
        Assert.assertEquals(v3, response);

        // get non exist
        try {
            response = ClusterKvManager.getInstance().get(cupidSession, k4_not_exist);
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().contains("Row Not Found"));
        }

        // listByPrefix test
        Map<String, String> conf;
        conf = ClusterKvManager.getInstance().listByPrefix(cupidSession, "linxuewei");
        Assert.assertEquals(conf.size(), 2);
        conf = ClusterKvManager.getInstance().listByPrefix(cupidSession, "linxuewei.cupid");
        Assert.assertEquals(conf.size(), 1);
        conf = ClusterKvManager.getInstance().listByPrefix(cupidSession, "linxuewei.moye");
        Assert.assertEquals(conf.size(), 1);
        conf = ClusterKvManager.getInstance().listByPrefix(cupidSession, "leanken");
        Assert.assertEquals(conf.size(), 1);

        // clean up
        ClusterKvManager.getInstance().delete(cupidSession, k1);
        ClusterKvManager.getInstance().delete(cupidSession, k2);
        ClusterKvManager.getInstance().delete(cupidSession, k3);
    }
}
