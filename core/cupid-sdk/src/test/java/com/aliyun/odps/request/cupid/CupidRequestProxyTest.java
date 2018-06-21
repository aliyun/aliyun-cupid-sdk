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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


import com.aliyun.odps.Odps;
import com.aliyun.odps.cupid.CupidConf;
import com.aliyun.odps.cupid.CupidSession;

import java.io.IOException;

/**
 * Created by liwei.li on 8/2/16.
 */
public class CupidRequestProxyTest {
    static Odps odps ;
    static String lookupName ;

    @BeforeClass
    public static void initCupidSession(){
        CupidConf cupidConf = new CupidConf();
        cupidConf.set("odps.access.id","");
        cupidConf.set("odps.access.key","");
        cupidConf.set("odps.end.point","");
        cupidConf.set("odps.project.name","");
        cupidConf.set("com.aliyun.biz_id", "1");

        CupidSession.setConf(cupidConf);
        CupidSession.get().setJobLookupName("");
        odps = CupidSession.get().odps;
        lookupName = CupidSession.get().getJobLookupName();
    }

//    @Test
    public void testRequestCupid() throws IOException {
        String requestResponse = CupidRequestProxy.getInstance().cupidRequestRPC("hahahha" , odps, lookupName);
        System.out.println(requestResponse);
        String requireRes = "hello cupid," + "hahahha";
        Assert.assertEquals(requireRes, requestResponse);
    }
}
