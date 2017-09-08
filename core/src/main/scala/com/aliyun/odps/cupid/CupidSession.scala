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

package com.aliyun.odps.cupid

import java.util.concurrent.atomic.AtomicInteger

import com.aliyun.odps.Odps
import com.aliyun.odps.account.{Account, AliyunAccount, BearerTokenAccount}
import com.aliyun.odps.cupid.CupidSession.CupidJobInstance
import com.aliyun.odps.cupid.runtime.RuntimeContext
import com.aliyun.odps.cupid.util.Utils
import org.apache.log4j.Logger

class CupidSession(
                    val conf: CupidConf) {

  val logger = Logger.getLogger(this.getClass().getName())
  private val cupidJobInstance = new CupidJobInstance("", false, new Object)
  var moyeFilesDir: String = Utils.createTempDir().getAbsolutePath
  var odps: Odps = initOdps
  var biz_id = conf.get("com.aliyun.biz_id", "")
  var flightingMajorVersion = conf.get("odps.task.major.version", null)
  var saveId = new AtomicInteger(0)

  def getJobLookupName: String = {
    this.cupidJobInstance.lookupName
  }

  def setJobLookupName(lookUpName: String) = {
    this.cupidJobInstance.lookupName = lookUpName
  }

  def setJobRunning(): Unit = {
    this.cupidJobInstance.jobRunningLock.synchronized {
      this.cupidJobInstance.jobisRunning = true
      this.cupidJobInstance.jobRunningLock.notifyAll()
    }
  }

  def getJobRunning(): Boolean = {
    val waitAmStartTime = this.conf.get("odps.cupid.wait.am.start.time", "600").toInt * 1000
    this.cupidJobInstance.jobRunningLock.synchronized {
      if (!this.cupidJobInstance.jobisRunning) {
        this.cupidJobInstance.jobRunningLock.wait(waitAmStartTime)
      }
    }
    this.cupidJobInstance.jobisRunning
  }

  def refreshOdps: Unit = {
    odps = initOdps
  }

  def initOdps: Odps = {
    val account = getAccount
    val odps = new Odps(account)
    odps.setEndpoint(conf.get("odps.end.point"))
    odps.setDefaultProject(conf.get("odps.project.name"))
    val runningCluster = conf.get("odps.moye.job.runningcluster", "")
    if (runningCluster != "") {
      logger.info("user set runningCluster=" + runningCluster)
      odps.instances().setDefaultRunningCluster(runningCluster);
    }
    odps
  }

  private def getAccount: Account = {
    if (System.getenv("META_LOOKUP_NAME") != null && conf.get("odps.cupid.bearer.token.enable", "true").toLowerCase == "true") {
      try {
        new BearerTokenAccount(RuntimeContext.get().getBearerToken())
      }
      catch {
        case ex: Exception => {
          logger.error(s"initialize BearerTokenAccount failed with ${ex.getMessage}, fallback to use AliyunAccount")
          new AliyunAccount(conf.get("odps.access.id"), conf.get("odps.access.key"))
        }
        case error: Throwable => {
          logger.error(s"initialize BearerTokenAccount failed with ${error.getMessage}, fallback to use AliyunAccount")
          new AliyunAccount(conf.get("odps.access.id"), conf.get("odps.access.key"))
        }
      }
    } else {
      new AliyunAccount(conf.get("odps.access.id"), conf.get("odps.access.key"))
    }
  }

}

object CupidSession {

  private val confLock = new Object
  var conf = new CupidConf()
  private var cupidSession: CupidSession = null

  def reset: Unit = {
    cupidSession = null
  }

  def get: CupidSession = {
    this.synchronized {
      if (cupidSession == null) {
        this.cupidSession = new CupidSession(this.getConf)
        cupidSession.setJobLookupName(System.getenv("META_LOOKUP_NAME"))
      }
      this.cupidSession
    }
  }

  def getConf: CupidConf = {
    this.confLock.synchronized {
      while (this.conf.getAll.length == 0) {
        // wait the conf to be set
        this.confLock.wait(20000)
      }
    }
    this.conf
  }

  def setConf(conf: CupidConf) {
    this.confLock.synchronized {
      this.conf = conf
      this.confLock.notifyAll()
    }
  }

  class CupidJobInstance(var lookupName: String, var jobisRunning: Boolean, val jobRunningLock: Object)

}
