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

package com.aliyun.odps.cupid.client.spark

import com.aliyun.odps.cupid.client.spark.api.{JobContext, SparkJob}
import com.typesafe.config.Config

class SparkJobKill extends SparkJob {
  override def runJob(jobContext: JobContext, jobConf: Config): Array[Byte] = {
    val sourceData = jobContext.sc().parallelize(0 to 100000, 2).map(i => {
      Thread.sleep(10000000)
      i
    }).count().toString.getBytes
    sourceData
  }
}
