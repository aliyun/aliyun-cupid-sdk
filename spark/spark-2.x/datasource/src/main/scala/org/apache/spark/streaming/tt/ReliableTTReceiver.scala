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

import java.util.concurrent.ConcurrentHashMap

import com.aliyun.odps.cupid.tools.Logging
import org.apache.spark.storage.{StorageLevel, StreamBlockId}
import org.apache.spark.streaming.receiver.{BlockGenerator, BlockGeneratorListener, Receiver}
import org.apache.spark.streaming.util.Decoder

import scala.actors.threadpool.{ExecutorService, Executors}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math._
import scala.reflect._

private[streaming]
class ReliableTTReceiver[T: ClassTag, U <: Decoder[_] : ClassTag](
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

  /**
    * A ArrayBuffer to store all the TTLogBlock for later ack, this is called in
    * synchronized block, so ArrayBuffer will not meet concurrency issue.
    */
  private var ttMsgAndMetaBuffer: ArrayBuffer[TTMessageAndMetadata[T]] = null

  /** A concurrent HashMap to store the stream block id and related msgAndMeta snapshot . */
  private var blockTTMsgAndMetaMap: ConcurrentHashMap[StreamBlockId, Array[TTMessageAndMetadata[T]]] = null

  /**
    * Manage the BlockGenerator in receiver itself for better managing block store and offset
    * commit.
    */
  private var blockGenerator: BlockGenerator = null

  def onStart(): Unit = {

    // wal offset management相关 初始化，可以参考org.apache.spark.streaming.kafka.ReliableKafkaReceiver
    ttMsgAndMetaBuffer = new ArrayBuffer[TTMessageAndMetadata[T]]()
    blockTTMsgAndMetaMap = new ConcurrentHashMap[StreamBlockId, Array[TTMessageAndMetadata[T]]]()
    blockGenerator = supervisor.createBlockGenerator(new GeneratedBlockHandler)

    val streams: ArrayBuffer[TTStream[T]] =
      TTUtils.createTTStreamConnector[T, U](topic, subId, accessKey, specificQueues, streamParallelism, maxQueueBlockSize)

    messageHandlerThreadPool = Executors.newFixedThreadPool(streams.length)

    // 启动blockGenerator
    blockGenerator.start()

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
      logDebug("Starting ReliableTTStream MessageHandler.")
      try {
        val streamIterator = stream.iterator()
        while (streamIterator.hasNext()) {
          val msgAndMeta: TTMessageAndMetadata[T] = streamIterator.next()
          storeMessageAndMetadata(msgAndMeta)
        }
      } catch {
        case e: Throwable => reportError("Error handling message; exiting", e)
      }
    }
  }

  /** Store a TT message and the associated metadata. */
  private def storeMessageAndMetadata(msgAndMetadata: TTMessageAndMetadata[T]): Unit = {
    val data = msgAndMetadata.message()

    // 存入Message以及Meta信息，Meta信息用于Callback以后的本地存储，用于后续的ACK操作
    blockGenerator.addDataWithCallback(data, msgAndMetadata)
  }

  /** Update stored local TTMsgAndMetaBuffer so as to track every TTLogBlock for later ACK */
  private def updateMsgAndMetaBuffer(ttBlockRecord: TTMessageAndMetadata[T]): Unit = {
    ttMsgAndMetaBuffer.append(ttBlockRecord)
  }

  /**
    * Remember the stream Block and relate TTMsgAndMeta Records when a block has been generated
    */
  private def rememberBlockMsgBufferMapping(blockId: StreamBlockId): Unit = {
    // Get a snapshot of current buffer map and store with related block id.
    val recordSnapshot = ttMsgAndMetaBuffer.toArray
    blockTTMsgAndMetaMap.put(blockId, recordSnapshot)
    ttMsgAndMetaBuffer.clear()
  }

  /**
    * Store the ready-to-be-stored block and commit the related offsets. This method
    * will try a fixed number of times to push the block. If the push fails, the receiver is stopped.
    */
  private def storeBlockAndCommitOffset(blockId: StreamBlockId, arrayBuffer: mutable.ArrayBuffer[_]): Unit = {
    var count = 0
    var pushed = false
    var exception: Exception = null
    while (!pushed && count <= 3) {
      try {
        store(arrayBuffer.asInstanceOf[mutable.ArrayBuffer[T]])
        pushed = true
      } catch {
        case ex: Exception =>
          count += 1
          exception = ex
      }
    }
    if (pushed) {
      blockTTMsgAndMetaMap.get(blockId).foreach(ttMsgAndMeta => {
        // Call TTLogBlock.getKey().ack()
        // 因为TT必须每一个Block都要ack，才能算是消费
        ttMsgAndMeta.commitOffset()
      })
      blockTTMsgAndMetaMap.remove(blockId)
    } else {
      stop("Error while storing block into Spark", exception)
    }
  }

  /** Class to handle blocks generated by the block generator. */
  private final class GeneratedBlockHandler extends BlockGeneratorListener {

    /**
      * 这里实现了四个钩子，分别是，存数据后，产生一个block之后，一个push完一个block之后，Error后的对应handler
      */

    def onAddData(data: Any, metadata: Any): Unit = {
      // Update the TTMsgAndMetaBuffer after data was added to the generator
      if (metadata != null) {
        val msgAndMeta = metadata.asInstanceOf[TTMessageAndMetadata[T]]
        updateMsgAndMetaBuffer(msgAndMeta)
      }
    }

    def onGenerateBlock(blockId: StreamBlockId): Unit = {
      // Remember the stream Block and relate TTMsgAndMeta Records when a block has been generated
      rememberBlockMsgBufferMapping(blockId)
    }

    def onPushBlock(blockId: StreamBlockId, arrayBuffer: mutable.ArrayBuffer[_]): Unit = {
      // Store block and commit every TTLogBlock's offset, by calling their ack
      storeBlockAndCommitOffset(blockId, arrayBuffer)
    }

    def onError(message: String, throwable: Throwable): Unit = {
      reportError(message, throwable)
    }
  }

}
