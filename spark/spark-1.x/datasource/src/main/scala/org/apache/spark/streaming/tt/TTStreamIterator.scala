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

import com.aliyun.odps.cupid.tools.Logging

/**
  * @author xuewei.linxuewei
  */


class State

object DONE extends State

object READY extends State

object NOT_READY extends State

object FAILED extends State

/**
  * 参考 kafka.utils.IteratorTemplate 觉得这个状态机还挺好玩的
  *
  */
class TTStreamIterator[T](queue: BlockingQueue[T]) extends Iterator[T] with java.util.Iterator[T] with Logging {
  private var state: State = NOT_READY
  private var nextItem = null.asInstanceOf[T]

  def next(): T = {
    if (!hasNext())
      throw new NoSuchElementException()
    state = NOT_READY
    if (nextItem == null)
      throw new IllegalStateException("Expected item but none found.")
    nextItem
  }

  def peek(): T = {
    if (!hasNext())
      throw new NoSuchElementException()
    nextItem
  }

  def hasNext(): Boolean = {
    if (state == FAILED)
      throw new IllegalStateException("Iterator is in failed state")
    state match {
      case DONE => false
      case READY => true
      case _ => maybeComputeNext()
    }
  }

  def makeNext(): T = {
    try {
      // TODO 阻塞获取 先不考虑timeout的问题, KafkaComsumer在timeout之后会把receiver任务做掉
      queue.take
    }
    catch {
      case e: Exception => {
        logError(s"TTMessageAndMetadata dequeue failed, exception: $e")
        throw e
      }
    }
  }

  def maybeComputeNext(): Boolean = {
    state = FAILED
    nextItem = makeNext()
    if (state == DONE) {
      false
    } else {
      state = READY
      true
    }
  }

  protected def allDone(): T = {
    state = DONE
    null.asInstanceOf[T]
  }

  def remove =
    throw new UnsupportedOperationException("Removal not supported")

  def resetState() {
    state = NOT_READY
  }
}
