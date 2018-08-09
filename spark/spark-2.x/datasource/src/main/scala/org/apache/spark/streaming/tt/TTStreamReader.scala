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

package org.apache.spark.streaming.tt

import java.util.concurrent.{BlockingQueue, TimeUnit}

import com.alibaba.tt.exception.{TTQueueException, TTQueueSafemodeException, TTRouterException, TTSubChangedException}
import com.alibaba.tt.log.impl.{TTLogBlock, TTLogSimpleInput}
import com.alibaba.tt.log.{TTLog, TTLogInput}
import com.aliyun.odps.cupid.tools.Logging
import org.apache.spark.streaming.util.Decoder

/**
  * @author xuewei.linxuewei
  */

class TTStreamReader[T](
     queue: BlockingQueue[TTMessageAndMetadata[T]],
     ttLog: TTLog,
     queueIndexes: Array[Int],
     decoder: Decoder[T]
   ) extends Runnable with Logging {

  private var input: TTLogInput = null

  def createInput(): Unit = {
    try {
      if (this.input != null) {
        this.input.close()
      }

      // 先销毁后创建，确保能work
      this.input = new TTLogSimpleInput(ttLog, queueIndexes)

    } catch {
      case e: TTQueueException => {
        logError(s"new TTLogSimpleInput failed with TTQueueException $e")
      }
      case e: Exception => {
        logError(s"new TTLogSimpleInput failed with Exception $e")
      }
    }
  }

  override def run(): Unit = {
    this.createInput()
    var block: TTLogBlock = null
    var emptyBlock: Boolean = false

    while (true) {
      try {
        emptyBlock = false
        block = this.input.read()
      } catch {
        case e: TTQueueException => {
          logError(s"input read failed $e")

          /**
            * 触发条件：
            * 1. 发生网络异常，或者TT服务端容灾切换
            * 2. TT降级，禁止读取
            */
          TimeUnit.MILLISECONDS.sleep(1000)
          this.createInput()
        }
        case e: TTQueueSafemodeException => {
          logError(s"input enter safe mode $e")

          /**
            * 触发条件：
            * 1. offset更新失败，通常情况是服务端开启了safe mode模式，可重试
            * 2. 多个客户端读同一个queue，会相互踢掉，需要保证任何时刻只有一个client订阅
            */
          TimeUnit.MILLISECONDS.sleep(1000)
        }
        case e: TTSubChangedException => {
          /**
            * 触发条件：
            * 在页面设置了订阅点位后，会抛出该异常
            */
          logError(s"input sub changed $e")
          this.createInput()
        }
        case e: TTRouterException => {
          logError(s"router exception $e")
          TimeUnit.MILLISECONDS.sleep(1000)
          this.createInput()
        }
      }

      if (block == null) {
        /**
          * block读到null，表示已经没有新数据了
          */
        TimeUnit.MILLISECONDS.sleep(1000)
        emptyBlock = true
      }

      if (!emptyBlock) {
        try {
          queue.put(new TTMessageAndMetadata[T](block, decoder, ttLog.getName, queueIndexes))
        } catch {
          case e: Exception => {
            logError(s"TTStreamReader enqueue failed with exception: $e")
            throw e
          }
        }
      }
    }

  }
}
