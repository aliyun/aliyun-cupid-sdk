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

package com.aliyun.odps.cupid.client.spark.api

import com.aliyun.odps.Odps
import org.apache.spark.SparkContext

/**
  * In runtime sparkjob  use the jobContext communicate with the sparkcontext or odps
  */
trait JobContext {
  /**
    * The sparkcontext can submit job etc.
    *
    * @return
    */
  def sc(): SparkContext

  /**
    * Can use the odps session communicate with odps frontend,eg: run a odps sql etc.
    *
    * @return
    */
  def odps(): Odps

  /**
    * Put the object in runtime,eg rdd,broadcast,dataframe
    *
    * @param objectName  use set the object name
    * @param namedObject the object
    */
  def putNamedObject(objectName: String, namedObject: Any)

  /**
    * Get the object any sparkjob putting
    *
    * @param objectName The object name
    * @return
    */
  def getNamedObject(objectName: String): Any

  def removeNamedObject(objectName: String)

}
