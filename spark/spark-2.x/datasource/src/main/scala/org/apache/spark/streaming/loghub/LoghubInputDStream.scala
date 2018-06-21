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

import com.aliyun.openservices.log.Client
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

/**
 * Loghub InputDStream
 *
 * User: 六翁 lu.hl@alibaba-inc.com
 * Date: 2017-01-16
 */
class LoghubInputDStream(@transient _ssc: StreamingContext, param:StreamingParam) extends ReceiverInputDStream[Array[Byte]](_ssc) {
  param.setInOrder(_ssc.sc.getConf.getBoolean("spark.logservice.fetch.inOrder", defaultValue = true))
  param.setHbInterval(_ssc.sc.getConf.getLong("spark.logservice.heartbeat.interval.millis", 30000L))
  param.setFetchInterval(_ssc.sc.getConf.getLong("spark.logservice.fetch.interval.millis", 200L))
  param.setBatchInterval(_ssc.graph.batchDuration.milliseconds)
  lazy val slsClient = new Client(param.getEndpoint, param.getId, param.getSecret)
  if (param.isForceSpecial && param.getCursor.toString.equals(LogHubCursorPosition.SPECIAL_TIMER_CURSOR.toString)) {
    try {
      slsClient.DeleteConsumerGroup(param.getProject, param.getLogstore, param.getGroup)
    } catch {
      case e: Exception =>
        logError(s"Failed to delete consumer group, ${e.getMessage}", e)
    }
  }

  override def getReceiver(): Receiver[Array[Byte]] = new LoghubReceiver(param:StreamingParam)
}