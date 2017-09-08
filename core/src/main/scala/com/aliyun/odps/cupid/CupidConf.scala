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

package com.aliyun.odps.cupid

import java.util.concurrent.ConcurrentHashMap

import org.apache.log4j.Logger

import scala.collection.JavaConverters._

class CupidConf() {

  val logger = Logger.getLogger(this.getClass().getName())
  private val settings = new ConcurrentHashMap[String, String]()

  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }

  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }

  def getBoolean(key: String, defaultValue: Boolean): Boolean = {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  def getOption(key: String): Option[String] = {
    Option(settings.get(key))
  }

  def getAll: Array[(String, String)] = {
    settings.entrySet().asScala.map(x => (x.getKey, x.getValue)).toArray
  }

  def remove(key: String): CupidConf = {
    settings.remove(key)
    this
  }

  def set(key: String, value: String): CupidConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
    settings.put(key.trim(), value.trim())
    this
  }
}