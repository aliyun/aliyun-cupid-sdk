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

package com.aliyun.odps.cupid.util

import java.io.{File, IOException}
import java.util.UUID

object Utils {

  private val MAX_DIR_CREATION_ATTEMPTS: Int = 10
  private val shutdownDeletePaths = new scala.collection.mutable.HashSet[String]()

  /**
    * Create a temporary directory inside the given parent directory. The directory will be
    * automatically deleted when the VM shuts down.
    */

  def createTempDir(
                     root: String = System.getProperty("java.io.tmpdir"),
                     namePrefix: String = "moye"): File = {
    val dir = createDirectory(root, namePrefix)
    registerShutdownDeleteDir(dir)
    dir
  }

  // Register the path to be deleted via shutdown hook
  def registerShutdownDeleteDir(file: File) {
    val absolutePath = file.getAbsolutePath()
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths += absolutePath
    }
  }

  def createDirectory(root: String, namePrefix: String = "moye"): File = {
    var attempts = 0
    val maxAttempts = MAX_DIR_CREATION_ATTEMPTS
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch {
        case e: SecurityException => dir = null;
      }
    }

    dir.getCanonicalFile
  }

  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  def getSparkClassLoader: ClassLoader = getClass.getClassLoader
}