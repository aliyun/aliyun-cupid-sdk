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

package com.aliyun.odps.cupid.client.spark.service

import com.aliyun.odps.cupid.CupidSession
import com.aliyun.odps.cupid.runtime.RuntimeContext
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

class CupidSparkRemoteDriver {
  private final val conf = new SparkConf().setAppName("CupidSparkRemoteDriver")
  private final val _sc = new SparkContext(conf)
  private final val shutdownLock = new java.util.concurrent.locks.ReentrantLock()
  private final val shutdownLoopCondition = shutdownLock.newCondition()
  val logger = Logger.getLogger(this.getClass().getName())
  private var running = true

  def sc(): SparkContext = {
    this._sc
  }

  def shutdownDriver(): Unit = {
    logger.info("Now stop the CupidSparkRemoteDriver")
    running = false
    withLock(shutdownLoopCondition.signal())
  }

  private def withLock[T](body: => T): T = {
    shutdownLock.lock()
    try body
    finally shutdownLock.unlock()
  }

  def run() {
    val jobContextImpl = new JobContextImpl(this._sc, CupidSession.get.odps)
    val jarInfoManager = new JarInfoManager()
    val jobInfoManager = new JobInfoManager()
    val sparkServiceContextImpl = new SparkServiceContextImpl(jobContextImpl, jobInfoManager, jarInfoManager)
    val sparkClientServiceImpl = new SparkClientServiceImpl(sparkServiceContextImpl, this)
    RuntimeContext.get().setCupidClientService(sparkClientServiceImpl)
    logger.info("Now the CupidSparkRemoteDriver start")
    withLock {
      while (running) shutdownLoopCondition.await()
    }
    logger.info("Now the CupidSparkRemoteDriver finish")
  }
}

object CupidSparkRemoteDriver {
  def main(args: Array[String]) {
    new CupidSparkRemoteDriver().run()
  }
}