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

import com.aliyun.odps.cupid.tools.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.util.Decoder

import scala.actors.threadpool.{ExecutorService, Executors}
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * @author xuewei.linxuewei
  */

private[streaming]
class TTInputDStream[T: ClassTag, U <: Decoder[_] : ClassTag](
     @transient ssc_ : StreamingContext,
     topic: String,
     subId: String,
     accessKey: String,
     useReliableReceiver: Boolean,
     specificQueues: Array[Int],
     streamParallelism: Int,
     maxQueueBlockSize: Int,
     storageLevel: StorageLevel
   ) extends ReceiverInputDStream[T](ssc_) {

  def getReceiver(): Receiver[T] = {
    if (!useReliableReceiver){
      new TTReceiver[T, U](topic, subId, accessKey, specificQueues, streamParallelism, maxQueueBlockSize, storageLevel)
    } else {
      new ReliableTTReceiver[T, U](topic, subId, accessKey, specificQueues, streamParallelism, maxQueueBlockSize, storageLevel)
    }
  }
}

private[streaming]
class TTReceiver[T: ClassTag, U <: Decoder[_] : ClassTag](
     topic: String,
     subId: String,
     accessKey: String,
     specificQueues: Array[Int],
     streamParallelism: Int,
     maxQueueBlockSize: Int,
     storageLevel: StorageLevel
   ) extends Receiver[T](storageLevel) with Logging {

  // TODO 暂时不考虑TT queue变化的情况 后续可以参考kafka的动态balance实现
  private var messageHandlerThreadPool: ExecutorService = null

  def onStart(): Unit = {

    val streams: ArrayBuffer[TTStream[T]] =
      TTUtils.createTTStreamConnector[T, U](topic, subId, accessKey, specificQueues, streamParallelism, maxQueueBlockSize)

    messageHandlerThreadPool = Executors.newFixedThreadPool(streams.length)
    try {
      // Start the messages handler for each partition
      streams.foreach { stream => messageHandlerThreadPool.submit(new MessageHandler(stream)) }
    } finally {
      messageHandlerThreadPool.shutdown() // Just causes threads to terminate after work is done
    }
  }

  def onStop(): Unit = {
    if (messageHandlerThreadPool != null) {
      messageHandlerThreadPool.shutdown()
      messageHandlerThreadPool = null
    }
  }

  // Handles TT messages
  private class MessageHandler(stream: TTStream[T])
    extends Runnable {
    def run() {
      logDebug("Starting TTStream MessageHandler.")
      try {
        val streamIterator = stream.iterator()
        while (streamIterator.hasNext()) {
          val msgAndMeta: TTMessageAndMetadata[T] = streamIterator.next()
          store(msgAndMeta.message())
          msgAndMeta.commitOffset()
        }
      } catch {
        case e: Throwable => reportError("Error handling message; exiting", e)
      }
    }
  }

}
