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

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaDStream, JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * Loghub Utils
 *
 * User: 六翁 lu.hl@alibaba-inc.com
 * Date: 2017-01-16
 */
object LoghubUtils {

  def createStream(ssc: StreamingContext, param: StreamingParam): ReceiverInputDStream[Array[Byte]] = {
    ssc.withNamedScope("loghub stream") {
      val appId = ssc.sc.applicationId
      param.setInstance(appId)
      new LoghubInputDStream(ssc, param)
    }
  }

  def createStream(ssc: StreamingContext, param: StreamingParam, numReceivers: Int): DStream[Array[Byte]] = {
    ssc.withNamedScope("loghub stream") {
      val appId = ssc.sc.applicationId
      param.setInstance(appId)
      ssc.union(Array.tabulate(numReceivers)(e => e).map(t => new LoghubInputDStream(ssc, param)))
    }
  }

  def createStream(jssc: JavaStreamingContext, param: StreamingParam): JavaReceiverInputDStream[Array[Byte]] = {
    createStream(jssc.ssc, param)
  }

  def createStream(jssc: JavaStreamingContext, param: StreamingParam, numReceivers: Int): JavaDStream[Array[Byte]] = {
    createStream(jssc.ssc, param)
  }
}