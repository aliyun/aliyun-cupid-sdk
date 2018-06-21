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

import java.util.concurrent.BlockingQueue

import com.alibaba.tt.log.TTLog
import com.aliyun.odps.cupid.tools.Logging
import org.apache.spark.streaming.util.Decoder

import scala.actors.threadpool.Executors

/**
  * @author xuewei.linxuewei
  */

class TTStream[T](
     queue: BlockingQueue[TTMessageAndMetadata[T]],
     ttLog: TTLog,
     queueIndexes: Array[Int],
     decoder: Decoder[T]
   ) extends Iterable[TTMessageAndMetadata[T]] with java.lang.Iterable[TTMessageAndMetadata[T]] with Logging {

  private val iter: TTStreamIterator[TTMessageAndMetadata[T]] = new TTStreamIterator[TTMessageAndMetadata[T]](queue)
  override def iterator(): TTStreamIterator[TTMessageAndMetadata[T]] = iter

  logInfo(s"TTStream Constructor queueIndexex: ${queueIndexes.mkString(",")}")
  // 启动TTStreamReader线程去拉去TT数据到queue
  Executors.newSingleThreadExecutor().submit(new TTStreamReader[T](queue, ttLog, queueIndexes, decoder))
}



