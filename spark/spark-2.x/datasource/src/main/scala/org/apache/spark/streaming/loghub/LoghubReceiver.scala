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

package org.apache.spark.streaming.loghub

import com.aliyun.openservices.loghub.client.ClientWorker
import com.aliyun.openservices.loghub.client.config.{LogHubConfig, LogHubCursorPosition}
import com.aliyun.odps.cupid.tools.Logging
import org.apache.spark.streaming.receiver.Receiver

/**
 * Loghub Receiver
 *
 * User: 六翁 lu.hl@alibaba-inc.com
 * Date: 2017-01-16
 */
private[loghub] class LoghubReceiver(param: StreamingParam) extends Receiver[Array[Byte]](param.getLevel) with Logging {
  receiver =>

  private var workerThread: Thread = _
  private var worker: ClientWorker = _
  /*30秒是经验值 参考 https://help.aliyun.com/document_detail/28116.html*/
  private val WAITING_SHUTDOWN = 30 * 1000

  def getBatchInterval: Long = param.getBatchInterval

  override def onStart(): Unit = {
    val initCursor = param.getCursor
    val config = if (!param.getCursor.toString.equals(LogHubCursorPosition.SPECIAL_TIMER_CURSOR.toString)) {
      new LogHubConfig(param.getGroup, s"${param.getGroup}p-${param.getInstance()}-${streamId}", param.getEndpoint, param.getProject, param.getLogstore, param.getId, param.getSecret, initCursor, param.getHbInterval, param.isInOrder)
    } else {
      new LogHubConfig(param.getGroup, s"${param.getGroup}p-${param.getInstance()}-${streamId}", param.getEndpoint, param.getProject, param.getLogstore, param.getId, param.getSecret,  param.getStart, param.getHbInterval, param.isInOrder)
    }
    config.setDataFetchIntervalMillis(param.getFetchInterval)
    worker = new ClientWorker(new LogHubProcessorFactory(receiver), config)
    workerThread = new Thread(worker)
    workerThread.setName(s"SLS Loghub Receiver $streamId")
    workerThread.setDaemon(true)
    workerThread.start()
    logInfo(s"Started receiver with streamId $streamId")
  }

  override def onStop(): Unit = {
    if (workerThread != null) {
      if (worker != null) {
        worker.shutdown()
        Thread.sleep(WAITING_SHUTDOWN)
      }
      workerThread.join()
      workerThread = null
      logInfo(s"Stopped receiver for streamId $streamId")
    }
  }
}
