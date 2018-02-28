# Spark-On-Odps如何使用VolumeFs

## 确认Project是否支持VolumeFs

* 登录Admin Console -> Project管理
* 找到对应的Project -> 双击查看
* 此处必须有两个开关同时打开, 才可确认Volume2已支持
	* 第一个开关 **允许volumes File:**
	* 第二个开关 **ALLOWED_VOLUME2** 

## 配置VolumeFs-HDFS客户端

* 客户端下载地址: [hadoop-2.7.1.tar.gz](https://archive.apache.org/dist/hadoop/common/hadoop-2.7.1/hadoop-2.7.1.tar.gz)
* 下载VolumeFs相关库 [lib.tar.gz](https://raw.githubusercontent.com/aliyun/aliyun-cupid-sdk/master/VolumeFs/lib.tar.gz)
* 下载并解压客户端后, 需要设置HADOOP_HOME环境变量

```
# 添加以下代码到 .bashrc \ .bash_profile
export HADOOP_HOME=/path/to/hadoop-2.7.1/
```

* 编辑 <mark>$HADOOP_HOME/etc/hadoop/hadoop-env.sh</mark>

```
# 执行相关命令
mkdir -p $HADOOP_HOME/share/hadoop/odps

# 解压VolumeFs相关库到上面路径
tar xf /path/to/lib.tar.gz -C $HADOOP_HOME/share/hadoop/odps

# 添加以下代码到 $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# Odps Support
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$HADOOP_HOME/share/hadoop/odps/lib/*

```

* 配置 <mark>$HADOOP_HOME/etc/hadoop/core-site.xml</mark> PS. 所有TBD的部分都是要填写的

```
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

<!-- Put site-specific property overrides in this file. -->

<configuration>
    <property>
        <name>fs.odps.impl</name>
        <value>com.aliyun.odps.fs.VolumeFileSystem</value>
    </property>
    
    <property>
        <name>fs.AbstractFileSystem.odps.impl</name>
        <value>com.aliyun.odps.fs.VolumeFs</value>
    </property>
    
    <property>
        <name>io.file.buffer.size</name>
        <value>512000</value>
    </property>

     <property>
        <name>volume.internal</name>
        <value>false</value>
    </property>

    <property>
        <name>pangu.access.check</name>
        <value>false</value>
    </property>

    <property>
        <name>odps.access.id</name>
        <value>TBD</value>
    </property>

    <property>
        <name>odps.access.key</name>
        <value>TBD</value>
    </property>

    <property>
        <name>fs.defaultFS</name>
        <value>odps://<projectName>/</value> 
    </property>

    <property>
        <name>odps.service.endpoint</name>
        <value>TBD</value>
    </property>

    <property>
        <name>odps.end.point</name>
        <value>TBD</value>
    </property>

    <property>
        <name>odps.project.name</name>
        <value>TBD</value>
    </property>

    <property>
        <name>odps.tunnel.endpoint</name>
        <value>TBD</value>
    </property>
</configuration>
```

## VolumeFs-HDFS客户端演示

客户端配置完毕, 可以直接使用 `bin/hdfs dfs --help`

```
// 创建一个volume, volume必须在对应的project下面, volume1为volume名称
bin/hdfs dfs -mkdir odps://cupid_5kn/volume1/

// 创建volume下的一个文件夹
bin/hdfs dfs -mkdir odps://cupid_5kn/volume1/test_dir

// 上传文件
bin/hdfs dfs -put ~/abcd odps://cupid_5kn/volume1/test_dir/

// 查看文件
bin/hdfs dfs -cat odps://cupid_5kn/volume1/test_dir/abcd

// 等等其他操作...
```

## Spark Mllib使用VolumeFs案例

以KMeans案例作为参考

```
https://github.com/aliyun/aliyun-cupid-sdk/blob/master/examples/spark-examples/src/main/scala/com/aliyun/odps/spark/examples/mllib/KmeansModelSaveToVolume.scala

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

package com.aliyun.odps.spark.examples.mllib

import org.apache.spark.mllib.clustering.KMeans._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * KmeansModelSaveToVolume Volume
  * 1. build aliyun-cupid-sdk please set modelVolumeDir with real volume path
  * 2. properly set spark.defaults.conf must have [spark.hadoop.odps.cupid.volume.paths=odps://projectName/volumeName/]
  * 3. bin/spark-submit --master yarn-cluster --class com.aliyun.odps.spark.examples.mllib.KmeansModelSaveToVolume
  * /path/to/aliyun-cupid-sdk/examples/spark-examples/target/spark-examples_2.11-1.0.0-SNAPSHOT-shaded.jar
  */
object KmeansModelSaveToVolume {

  val projectName = "projectName"
  val volumeName = "volumeName"
  val modelVolumeDir = s"odps://$projectName/$volumeName/mllib_dir/"

  def main(args: Array[String]) {

    //1. train and save the model
    val spark = SparkSession
      .builder()
      .appName("KmeansModelSaveToVolume")
      .getOrCreate()

    val sc = spark.sparkContext
    val points = Seq(
      Vectors.dense(0.0, 0.0),
      Vectors.dense(0.0, 0.1),
      Vectors.dense(0.1, 0.0),
      Vectors.dense(9.0, 0.0),
      Vectors.dense(9.0, 0.2),
      Vectors.dense(9.2, 0.0)
    )
    val rdd = sc.parallelize(points, 3)
    val initMode = K_MEANS_PARALLEL
    val model = KMeans.train(rdd, k = 2, maxIterations = 2, runs = 1, initMode)
    val predictResult1 = rdd.map(feature => "cluster id: " + model.predict(feature) + " feature:" + feature.toArray.mkString(",")).collect
    println("modelVolumeDir=" + modelVolumeDir)
    model.save(sc, modelVolumeDir)

    //2. predict from the oss model
    val modelLoadOss = KMeansModel.load(sc, modelVolumeDir)
    val predictResult2 = rdd.map(feature => "cluster id: " + modelLoadOss.predict(feature) + " feature:" + feature.toArray.mkString(",")).collect
    assert(predictResult1.size == predictResult2.size)
    predictResult2.foreach(result2 => assert(predictResult1.contains(result2)))
  }
}


```