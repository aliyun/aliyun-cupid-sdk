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

package com.aliyun.odps.cupid.runtime

import apsara.odps.cupid.protocol.PartitionSplitFileInfo._
import apsara.odps.cupid.protocol.SavePathsProtos.SavePaths
import apsara.odps.cupid.protocol.StsTokenInfoProtos._
import com.aliyun.odps.data.{ArrayRecord, Record}
import com.aliyun.odps.{Column, TableSchema}
import com.google.protobuf.Service

case class CreateTableReaderResult(tableSchema: TableSchema, recordIterator: TableReaderIterator[Record]) {
}

trait TableWriterAbstract {
  def close()

  def getCols(): Array[Column]

  def writeRecord(record: ArrayRecord)

  def bytesWrite(): Long

  def recordsWrite(): Long
}

abstract class RuntimeContext {
  def getCupidClientService(serviceName: String): Service

  def setCupidClientService(service: Service)

  def stopChannelServer()

  def addRpcRequestHandler(handler: RPCRequestHandleWrapper)

  def getRpcRequestHandler(method: String): RPCRequestHandleWrapper

  @throws(classOf[Exception])
  def reportApplicationMasterFinishStatus(progress: Int, status: String)

  @throws(classOf[Exception])
  def reportContainerStatus(status: ContainerStatus, message: String)

  @throws(classOf[Exception])
  def createTableWriterByLabel(schema: String, fileName: String, label: String): TableWriterAbstract = {
    null.asInstanceOf[TableWriterAbstract]
  }

  @throws(classOf[Exception])
  def updateInputOutputInfos(inputOutputInfosManager: InputOutputInfosManager)

  @throws(classOf[Exception])
  def generatePartitonInfo(tableInputId: String, splitTempDir: String): PartitionSplitInfo

  @throws(classOf[Exception])
  def createTableReader(tableInputId: String, partitionId: String, splitStart: String, splitEnd: String, schemaSplitStart: String, schemaSplitEnd: String, splitTempDir: String): CreateTableReaderResult

  @throws(classOf[Exception])
  def commitTaskRenamePaths(saveResults: SavePaths, taskAttemptId: String)

  @throws(classOf[Exception])
  def rpcRequestWrapper(parameter: Array[Byte]): String

  @throws(classOf[Exception])
  def getStsToken(roleArn: String, stsTokenType: String): GetStsTokenRes

  @throws(classOf[Exception])
  def getBearerToken(): String
}

object RuntimeContext {

  var context: Option[RuntimeContext] = None

  def set(other: RuntimeContext) {
    context = Option(other)
  }

  def get(): RuntimeContext = context.get
}
