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

package com.aliyun.odps.cupid.client.spark.util

import java.lang.reflect.Constructor

import org.slf4j.LoggerFactory

/**
  * A set of utilities for dynamically loading classes from a jar file, and saving the jar file.
  */
object JarUtils {
  lazy val logger = LoggerFactory.getLogger(getClass)

  /**
    * Loads a Scala object or class from a classloader.
    * See http://stackoverflow.com/questions/3216780/problem-reloading-a-jar-using-urlclassloader?lq=1
    * See http://stackoverflow.com/questions/8867766/scala-dynamic-object-class-loading
    *
    * @param classOrObjectName must be the fully qualified name of the Scala object or class that
    *                          implements the SparkJob trait. If an object is used, do not include the
    *                          trailing '$'.
    * @param loader            the ClassLoader to use to load the class or object.  Typically a URLClassLoader.
    * @return Function0[C] to obtain the object/class. Calling the function will return a reference to
    *         the object (for objects), or a new instance of a class (for classes) that implement the
    *         SparkJob trait.
    */
  def loadClassOrObject[C](classOrObjectName: String, loader: ClassLoader): () => C = {
    def fallBackToClass(): () => C = {
      val constructor = loadConstructor[C](classOrObjectName, loader)
      () => constructor.newInstance()
    }

    // Try loading it as an object first, if that fails, then try it as a class
    try {
      val objectRef = loadObject[C](classOrObjectName + "$", loader)
      () => objectRef
    } catch {
      case e: java.lang.ClassNotFoundException => fallBackToClass()
      case e: java.lang.ClassCastException => fallBackToClass()
      case e: java.lang.NoSuchMethodException => fallBackToClass()
      case e: java.lang.NoSuchFieldException => fallBackToClass()
    }
  }

  private def loadConstructor[C](className: String, loader: ClassLoader): Constructor[C] = {
    logger.info("Loading class {} using loader {}", className: Any, loader)
    val loadedClass = loader.loadClass(className).asInstanceOf[Class[C]]
    val result = loadedClass.getConstructor()
    if (loadedClass.getClassLoader != loader) {
      logger.error("Wrong ClassLoader for class {}: Expected {} but got {}", loadedClass.getName,
        loader.toString, loadedClass.getClassLoader)
    }
    result
  }

  private def loadObject[C](objectName: String, loader: ClassLoader): C = {
    logger.info("Loading object {} using loader {}", objectName: Any, loader)
    val loadedClass = loader.loadClass(objectName)
    val objectRef = loadedClass.getField("MODULE$").get(null).asInstanceOf[C]
    if (objectRef.getClass.getClassLoader != loader) {
      logger.error("Wrong ClassLoader for object {}: Expected {} but got {}", objectRef.getClass.getName,
        loader.toString, objectRef.getClass.getClassLoader)
    }
    objectRef
  }

  //Jars and Eggs are both zip files
  def binaryIsZip(binaryBytes: Array[Byte]): Boolean = {
    binaryBytes.length > 4 &&
      // For now just check the first few bytes are the ZIP signature: 0x04034b50 little endian
      binaryBytes(0) == 0x50 && binaryBytes(1) == 0x4b && binaryBytes(2) == 0x03 && binaryBytes(3) == 0x04
  }

  /**
    * Find the JAR from which a given class was loaded, to make it easy for users to pass
    * their JARs to SparkContext.
    */
  def jarOfClass(cls: Class[_]): Option[String] = {
    val uri = cls.getResource("/" + cls.getName.replace('.', '/') + ".class")
    if (uri != null) {
      val uriStr = uri.toString
      if (uriStr.startsWith("jar:file:")) {
        // URI will be of the form "jar:file:/path/foo.jar!/package/cls.class",
        // so pull out the /path/foo.jar
        Some(uriStr.substring("jar:file:".length, uriStr.indexOf('!')))
      } else {
        None
      }
    } else {
      None
    }
  }
}
