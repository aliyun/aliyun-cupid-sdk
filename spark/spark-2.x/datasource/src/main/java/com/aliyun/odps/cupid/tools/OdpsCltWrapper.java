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

package com.aliyun.odps.cupid.tools;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.*;

/**
 * Created by admin on 11/2/16.
 */
public class OdpsCltWrapper {
  public static Logger logger = Logger.getLogger(OdpsCltWrapper.class.getClass().getName());
  public static void main(String[] args) throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    // for spark 2.1.0
    String javaClassPath = System.getProperty("java.class.path");
    logger.info("OdpsCltWrapper ClassPath: " + javaClassPath);
    Class sparkSubmitCls = null;
    URLClassLoader cl = null;
    Path outputDir = null;

    try {
      sparkSubmitCls = Class.forName("org.apache.spark.deploy.SparkSubmit");
      URL url = sparkSubmitCls.getProtectionDomain().getCodeSource().getLocation();
      cl = new URLClassLoader(new URL[]{url}, Thread.currentThread().getContextClassLoader().getParent());
      Thread.currentThread().setContextClassLoader(cl);
    }
    catch (ClassNotFoundException ex) {
      logger.info("ClassPath: " + javaClassPath + " can not found org.apache.spark.deploy.SparkSubmit, try find __spark_libs__.zip in ClassPath");

      // start find __spark_libs__.zip and add it to URLClassLoader
      String sparkLibZipPath = tryFindSparkLibZip(javaClassPath);

      // unzip to temporary directory
      outputDir = Files.createTempDirectory("__spark_libs__");
      URL[] urls = tryExtractSparkLibZip(sparkLibZipPath, outputDir.toAbsolutePath().toString());

      // ClassLoader Isolation
      cl = new URLClassLoader(urls, Thread.currentThread().getContextClassLoader().getParent());
      Thread.currentThread().setContextClassLoader(cl);
    }

    try {
      // since spark-2.1.0 odps.*.* config set in properties must be set to spark.hadoop.odps.*.*
      setProperpiesForD2();

      // SparkSubmit.main(args); change to reflect call, because SystemClassLoader currently can not found SparkSubmit
      sparkSubmitCls = Class.forName("org.apache.spark.deploy.SparkSubmit", true, cl);
      sparkSubmitCls.getMethod("main", new Class[]{String[].class}).invoke(sparkSubmitCls, new Object[]{args});
    }
    finally {
      // remove tmp spark_lib.zip
      FileUtils.deleteDirectory(new File(outputDir.toAbsolutePath().toString()));
    }
  }

  private static URL[] tryExtractSparkLibZip(String sparkLibZipPath, String extractTo) throws IOException {
    java.util.zip.ZipFile zipFile = new ZipFile(sparkLibZipPath);
    List<URL> urls = new ArrayList<>();

    try {
      Enumeration<? extends ZipEntry> entries = zipFile.entries();
      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();
        File entryDestination = new File(extractTo, entry.getName());
        urls.add(entryDestination.toURI().toURL());
        if (entry.isDirectory()) {
          entryDestination.mkdirs();
        } else {
          entryDestination.getParentFile().mkdirs();
          InputStream in = zipFile.getInputStream(entry);
          OutputStream out = new FileOutputStream(entryDestination);
          IOUtils.copy(in, out);
          IOUtils.closeQuietly(in);
          out.close();
        }
      }
    } finally {
      zipFile.close();
    }

    return urls.toArray(new URL[urls.size()]);
  }

  private static String tryFindSparkLibZip(String javaClassPath) throws IOException {
    String[] classPaths = javaClassPath.split(":");
    Boolean findSpark2Archives = false;
    String sparkLibZipPath = "";

    for (String i : classPaths) {
      if (i.toLowerCase().endsWith(".zip")) {
        // __spark_libs__.zip
        ZipFile zip = new ZipFile(i);
        Enumeration<? extends ZipEntry> entrys = zip.entries();
        while (entrys.hasMoreElements()) {
          ZipEntry entry = entrys.nextElement();
          if (entry.getName().toLowerCase().startsWith("spark-odps_2.11")) {
            // this means current file is __spark_libs__.zip
            findSpark2Archives = true;
            sparkLibZipPath = i;
            break;

          }
        }
        zip.close();
      }
      else
      {
        continue;
      }

      if (findSpark2Archives) {
        break;
      }
    }

    if (!findSpark2Archives) {
      throw new IOException("can not found __spark_libs__.zip in classpath: " + javaClassPath);
    }
    else
    {
      return sparkLibZipPath;
    }
  }

  private static void setProperpiesForD2()
  {
    String[] keys = {"odps.project.name", "odps.access.id", "odps.access.key", "odps.end.point", "odps.runtime.end.point"};
    for (String key : keys) {
      if (System.getProperties().containsKey(key)) {
        logger.info("setProperpiesForD2 transform key " + key + " to spark.hadoop." + key);
        System.setProperty("spark.hadoop." + key, System.getProperty(key));
      }
    }
  }
}
