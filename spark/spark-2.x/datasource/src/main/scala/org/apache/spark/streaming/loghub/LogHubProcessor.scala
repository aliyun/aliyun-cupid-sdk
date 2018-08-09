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

import java.util
import com.alibaba.fastjson.JSONObject
import com.aliyun.openservices.log.common.LogGroupData
import com.aliyun.openservices.log.common.Logs.Log
import com.aliyun.openservices.loghub.client.ILogHubCheckPointTracker
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor
import scala.collection.JavaConversions._

/**
 * Loghub Processor
 *
 * User: 六翁 lu.hl@alibaba-inc.com
 * Date: 2017-01-16
 */
class LogHubProcessor(receiver: LoghubReceiver) extends ILogHubProcessor {
  System.setProperty("file.encoding", "UTF-8")
  System.setProperty("sun.jnu.encoding", "UTF-8")

  private var mShardId: Int = 0
  private var mLastCheckTime = 0L
  private val __TIME__ = "__time__"
  private val __TOPIC__ = "__topic__"
  private val __SOURCE__ = "__source__"

  override def shutdown(iLogHubCheckPointTracker: ILogHubCheckPointTracker): Unit = {
    iLogHubCheckPointTracker.saveCheckPoint(true)
  }

  override def initialize(mShardId: Int): Unit = {
    this.mShardId = mShardId
  }

  override def process(list: util.List[LogGroupData], iLogHubCheckPointTracker: ILogHubCheckPointTracker): String = {
    try {
      list.foreach(group => {
        group.GetLogGroup().getLogsList.foreach(log => {
          process(group, log)
        })
      })
      val ct = System.currentTimeMillis()
      (ct - mLastCheckTime) > receiver.getBatchInterval match {
        /*https://spark.apache.org/docs/latest/streaming-custom-receivers.html#receiver-reliability*/
        case true =>
          iLogHubCheckPointTracker.saveCheckPoint(true)
          mLastCheckTime = ct
        case false =>
          iLogHubCheckPointTracker.saveCheckPoint(false)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    ""
  }

  private def process(group: LogGroupData, log: Log): Unit = {
    try {
      val topic = group.GetLogGroup.getTopic
      val source = group.GetLogGroup.getSource
      val obj = new JSONObject()
      obj.put(__TIME__, Integer.valueOf(log.getTime))
      obj.put(__TOPIC__, topic)
      obj.put(__SOURCE__, source)
      log.getContentsList.foreach(content => {
        obj.put(content.getKey, content.getValue)
      })
      receiver.store(obj.toJSONString.getBytes("UTF-8"))
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }
}

