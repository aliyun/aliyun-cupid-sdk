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

package com.aliyun.odps.cupid.client.spark.service

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import com.aliyun.odps.Odps
import com.aliyun.odps.cupid.client.spark.api.JobContext
import org.apache.spark.SparkContext

class JobContextImpl(sparkcontext: SparkContext, _odps: Odps) extends JobContext {
  private val namedObjects: ConcurrentMap[String, Any] = new ConcurrentHashMap[String, Any]

  override def sc(): SparkContext = {
    this.sparkcontext
  }

  override def odps(): Odps = {
    this._odps
  }

  override def getNamedObject(objectName: String): Any = {
    this.namedObjects.get(objectName)
  }

  override def putNamedObject(objectName: String, namedObject: Any): Unit = {
    this.namedObjects.put(objectName, namedObject)
  }

  override def removeNamedObject(objectName: String): Unit = {
    this.namedObjects.remove(objectName)
  }
}
