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

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.BearerTokenAccount;
import com.aliyun.odps.cupid.runtime.RuntimeContext;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

class CupidJobInstance {
    public String lookupName;
    public boolean jobisRunning;
    public Object jobRunningLock;

    public CupidJobInstance(String lookupName, boolean jobisRunning, Object jobRunningLock) {
        this.lookupName = lookupName;
        this.jobisRunning = jobisRunning;
        this.jobRunningLock = jobRunningLock;
    }
}

public class CupidSession {
    private Logger logger = Logger.getLogger(CupidSession.class);
    private CupidJobInstance cupidJobInstance = new CupidJobInstance("", false, new Object());

    public AtomicInteger saveId = null;
    public CupidConf conf = null;
    public Odps odps = null;

    // fetch from conf
    public String biz_id;
    public String flightingMajorVersion;

    public CupidSession(CupidConf conf) {
        this.conf = conf;
        this.biz_id = conf.get("com.aliyun.biz_id", "");
        flightingMajorVersion = conf.get("odps.task.major.version", null);
        saveId = new AtomicInteger(0);
        odps = initOdps();
    }

    public Odps odps()
    {
        refreshOdps();
        return this.odps;
    }

    public void setJobLookupName(String lookUpName) {
        this.cupidJobInstance.lookupName = lookUpName;
    }

    public String getJobLookupName() {
        return this.cupidJobInstance.lookupName;
    }

    public void setJobRunning() {
        synchronized (this.cupidJobInstance.jobRunningLock) {
            this.cupidJobInstance.jobisRunning = true;
            this.cupidJobInstance.jobRunningLock.notifyAll();
        }
    }

    public boolean getJobRunning() throws InterruptedException {
        int waitAmStartTime =
                Integer.parseInt(this.conf.get("odps.cupid.wait.am.start.time", "600")) * 1000;
        synchronized (this.cupidJobInstance.jobRunningLock) {
            if (!this.cupidJobInstance.jobisRunning) {
                this.cupidJobInstance.jobRunningLock.wait(waitAmStartTime);
            }
        }
        return this.cupidJobInstance.jobisRunning;
    }

    private Account getAccount() {
        if (System.getenv("META_LOOKUP_NAME") != null &&
                conf.get("odps.cupid.bearer.token.enable", "true").toLowerCase().equals("true")) {
            try {
                return new BearerTokenAccount(RuntimeContext.get().getBearerToken());
            } catch (Exception ex) {
                logger.error(
                        String.format(
                                "initialize BearerTokenAccount failed with %s, fallback to use AliyunAccount", ex.getMessage()
                        )
                );
                return new AliyunAccount(conf.get("odps.access.id"), conf.get("odps.access.key"));
            } catch (Throwable error) {

                logger.error(
                        String.format("initialize BearerTokenAccount failed with $s, fallback to use AliyunAccount",
                                error.getMessage())
                );
                return new AliyunAccount(conf.get("odps.access.id"), conf.get("odps.access.key"));
            }
        } else {
            return new AliyunAccount(conf.get("odps.access.id"), conf.get("odps.access.key"));
        }
    }

    public void refreshOdps() {
        this.odps = initOdps();
    }

    public Odps initOdps() {
        Account account = getAccount();
        Odps odps = new Odps(account);
        String lookupName = System.getenv("META_LOOKUP_NAME");
        if (lookupName == null) {
            odps.setEndpoint(conf.get("odps.end.point"));
        } else {
            odps.setEndpoint(conf.get("odps.runtime.end.point", conf.get("odps.end.point")));
        }
        odps.setDefaultProject(conf.get("odps.project.name"));
        String runningCluster = conf.get("odps.moye.job.runningcluster", "");
        if (!runningCluster.equals("")) {
            logger.info("user set runningCluster = " + runningCluster);
            odps.instances().setDefaultRunningCluster(runningCluster);
        }
        return odps;
    }

    private static Object confLock = new Object();
    private static CupidSession cupidSession = null;
    public static CupidConf globalConf = new CupidConf();

    public static void setConf(CupidConf conf) {
        synchronized (CupidSession.confLock) {
            globalConf= conf;
            confLock.notifyAll();
        }
    }

    public static CupidConf getConf() throws InterruptedException {
        synchronized (CupidSession.confLock) {
            while (CupidSession.globalConf.getAll().length == 0) {
                //wait the conf to be set
                CupidSession.confLock.wait(20000);
            }
        }
        return globalConf;
    }

    public static void reset() {
        cupidSession = null;
    }

    public static CupidSession get() {
        synchronized (CupidSession.class) {
            if (cupidSession == null) {
                try
                {
                    cupidSession = new CupidSession(getConf());
                }
                catch (Exception ex)
                {
                    ex.printStackTrace();

                }
                String lookupName = System.getenv("META_LOOKUP_NAME");
                if (lookupName == null) {
                    lookupName = "";
                }
                cupidSession.setJobLookupName(lookupName);
            }
            return cupidSession;
        }
    }
}
