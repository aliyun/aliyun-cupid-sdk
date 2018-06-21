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

import java.util.Properties
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import com.alibaba.tt.log.TTLog
import com.alibaba.tt.log.impl.TTLogImpl
import com.aliyun.odps.cupid.tools.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.util.{Decoder, StringDecoder, VerifiableProperties, WriteAheadLogUtils}

import scala.Array._
import scala.collection.mutable.ArrayBuffer
import scala.math._
import scala.reflect._

/**
  * @author xuewei.linxuewei
  * TT DataSource as StreamInput Util
  */

object TTUtils extends Logging {

  /**
    *
    * @param ssc
    * @param topic
    * @param subId
    * @param accessKey
    * @param streamParallelism
    * @param maxQueueBlockSize
    * @param storageLevel
    * @return
    */
  def createStream(
      ssc: StreamingContext,
      topic: String,
      subId: String,
      accessKey: String,
      streamParallelism: Int = -1,
      maxQueueBlockSize: Int = 16,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): ReceiverInputDStream[String] = {
    val walEnabled = WriteAheadLogUtils.enableReceiverLog(ssc.conf)
    logInfo(s"createStream walEnabled: ${walEnabled}")
    new TTInputDStream[String, StringDecoder](
      ssc,
      topic,
      subId,
      accessKey,
      walEnabled,
      Array[Int](),
      streamParallelism,
      maxQueueBlockSize,
      storageLevel)
  }

  /**
    *
    * @param ssc
    * @param topic
    * @param subId
    * @param accessKey
    * @param specificQueues
    * @param streamParallelism
    * @param maxQueueBlockSize
    * @param storageLevel
    * @return
    */
  def createStreamWithSpecificQueues(
      ssc: StreamingContext,
      topic: String,
      subId: String,
      accessKey: String,
      specificQueues: Array[Int],
      streamParallelism: Int = -1,
      maxQueueBlockSize: Int = 16,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): ReceiverInputDStream[String] = {
    val walEnabled = WriteAheadLogUtils.enableReceiverLog(ssc.conf)
    logInfo(s"createStream walEnabled: ${walEnabled}")
    new TTInputDStream[String, StringDecoder](
      ssc,
      topic,
      subId,
      accessKey,
      walEnabled,
      specificQueues,
      streamParallelism,
      maxQueueBlockSize,
      storageLevel)
  }

  /**
    * TT拉数据并行度决定
    *
    * @param streamParallelism 用户指定的并行度，不得超过topic QueueNumber数量
    * @param queueNumber topic获取的queueNumber
    */
  def getStreamParallelism(streamParallelism: Int, queueNumber: Int): Int = {
    // streamParallelism == -1是系统默认值，表示会每一个TT queue单独给一个线程去拉数据
    // 如用户需要自己指定，请确认streamParallelism <= ttLog.getQueueNumber()
    // 一个executor如果资源超用会被tubo杀掉, 按照一个并发stream来算，需要用到2个线程，故这里采取workerCores * 1.5 / 2, 按照超用1.5的标准来算
    // 也就是用户如果自己设定的streamParallelism， 最好少于workerCores的一半
    val workerCores = (sys.env.getOrElse("workerCores", queueNumber.toString * 100)).toInt
    logInfo(s"workerCores: ${workerCores} inside getStreamParallelism")

    if (streamParallelism != -1 && streamParallelism > 0) {
      return min(min(streamParallelism, queueNumber), round(workerCores / 100 * 1.5 / 2).toInt)
    }
    else {
      return min(queueNumber, round(workerCores / 100 * 1.5 / 2).toInt)
    }
  }

  /**
    *
    * @param topic
    * @param subId
    * @param accessKey
    * @param specificQueues
    * @param streamParallelism
    * @param maxQueueBlockSize
    * @tparam T
    * @tparam U
    * @return
    */
  def createTTStreamConnector[T: ClassTag, U <: Decoder[_] : ClassTag](
      topic: String,
      subId: String,
      accessKey: String,
      specificQueues: Array[Int],
      streamParallelism: Int,
      maxQueueBlockSize: Int): ArrayBuffer[TTStream[T]] = {

    val ttLog: TTLog = new TTLogImpl(topic, subId, accessKey)
    val queueNumber: Int = specificQueues.length match {
      case 0 => ttLog.getQueueNumber
      case _ => specificQueues.length
    }
    val currentParallelism: Int = getStreamParallelism(streamParallelism, queueNumber)

    logInfo(s"workerCores: ${sys.env.get("workerCores")}, " +
      s"streamParallelism: ${streamParallelism}, " +
      s"queueNumber: ${queueNumber}, " +
      s"currentParallelism: ${currentParallelism}, " +
      s"topic: ${topic}, " +
      s"subId: ${subId}, " +
      s"accessKey: ${accessKey}, " +
      s"maxQueueBlockSize: ${maxQueueBlockSize}, " +
      s"specificQueues: ${specificQueues.mkString(",")}")

    val streams: ArrayBuffer[TTStream[T]] = ArrayBuffer[TTStream[T]]()
    val queue: BlockingQueue[TTMessageAndMetadata[T]] =
      new LinkedBlockingQueue[TTMessageAndMetadata[T]](maxQueueBlockSize)

    // 根据范型U生成对应的Decode实例
    val props = new VerifiableProperties(new Properties())
    val decoder =
      classTag[U].runtimeClass.getConstructor(classOf[VerifiableProperties]).newInstance(props).asInstanceOf[Decoder[T]]

    // 一个topic多个并发拉去数据进程共享一个blockQueue，以供Receiver消费
    // 如果一个topic有7个queue，默认并发度是7，每一个queue一个stream
    // 如果并发度用户设置成3, 那就会生成3个stream，如下
    // new TTStream[T](queue, ttLog, Array[Int](0, 3, 6), decoder))
    // new TTStream[T](queue, ttLog, Array[Int](1, 4), decoder))
    // new TTStream[T](queue, ttLog, Array[Int](2, 5), decoder))

    // 如果用户指定了specificQueues 为 [0,1,4,5,7,9]
    // 如果并发度用户设置成3, 那就会生成3个stream，如下
    // new TTStream[T](queue, ttLog, Array[Int](0, 5), decoder))
    // new TTStream[T](queue, ttLog, Array[Int](1, 7), decoder))
    // new TTStream[T](queue, ttLog, Array[Int](4, 9), decoder))
    val queueIndexs: Array[Int] = specificQueues.length match {
      case 0 => range(0, ttLog.getQueueNumber, 1)
      case _ => specificQueues
    }

    for (i <- 0 to currentParallelism - 1) {
      streams.append(new TTStream[T](queue, ttLog, range(i, queueNumber, currentParallelism).map(queueIndexs(_)), decoder))
    }

    streams
  }

}
