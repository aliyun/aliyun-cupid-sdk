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

package com.aliyun.odps.cupid.client.spark.util.serializer

import java.util.{HashMap, Iterator, Map}

object TestJavaSerializerInstance {
  def main(args: Array[String]): Unit = {
    val testMap: Map[String, String] = new HashMap[String, String]
    testMap.put("1", "hello")
    testMap.put("2", "world")
    val serResult = JavaSerializerInstance.getInstance.serialize(testMap)
    println("size = " + serResult.array().length)
    val deserResult = JavaSerializerInstance.getInstance.deserialize[HashMap[String, String]](serResult)

    val mapsetIter: Iterator[Map.Entry[String, String]] = deserResult.entrySet.iterator
    while (mapsetIter.hasNext) {
      val iter: Map.Entry[String, String] = mapsetIter.next
      System.out.println("key = " + iter.getKey + ",value" + iter.getValue)
    }

  }
}
