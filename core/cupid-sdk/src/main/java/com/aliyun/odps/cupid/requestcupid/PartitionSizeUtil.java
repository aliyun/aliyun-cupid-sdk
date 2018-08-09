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

package com.aliyun.odps.cupid.requestcupid;

import apsara.odps.cupid.protocol.CupidTaskParamProtos.*;
import com.aliyun.odps.cupid.CupidSession;
import org.apache.log4j.Logger;

public class PartitionSizeUtil {

    private static Logger logger = Logger.getLogger(PartitionSizeUtil.class);

    private static CupidTaskParam.Builder getPartitionSizeBaseInfo()
    {

        CupidTaskParam.Builder cupidTaskParamBuilder =
                CupidTaskBaseUtil.getOperationBaseInfo(
                        CupidSession.get(),
                        CupidTaskOperatorConst.CUPID_TASK_GETPARTITIONSIZE,
                        CupidSession.get().getJobLookupName(),
                        null
                );
        return cupidTaskParamBuilder;
    }


    public static GetPartitionSizeResult getMultiTablesPartitonSize(MultiTablesInputInfos multiTablesInputInfos) throws Exception {
        CupidTaskParam.Builder cupidTaskParamBuilder = getPartitionSizeBaseInfo();
        cupidTaskParamBuilder.setMultiTablesInputInfos(multiTablesInputInfos);
        return SubmitJobUtil.getPartitionSizeSubmitJob(cupidTaskParamBuilder.build());
    }

    public static GetPartitionSizeResult getPartitonSize(
            String project,
            String table,
            String[] columns,
            String[] partition,
            int splitSize,
            int splitCount
    ) throws Exception {
        return getPartitonSize(project, table, columns, partition, splitSize, splitCount, 0, "");
    }

    public static GetPartitionSizeResult getPartitonSize(
            String project,
            String table,
            String[] columns,
            String[] partition,
            int splitSize,
            int splitCount,
            int odpsRddId
    ) throws Exception {
        return getPartitonSize(project, table, columns, partition, splitSize, splitCount, odpsRddId,"");
    }

    public static GetPartitionSizeResult getPartitonSize(
            String project,
            String table,
            String[] columns,
            String[] partition,
            int splitSize,
            int splitCount,
            int odpsRddId,
            String splitTempDir) throws Exception {
        MultiTablesInputInfos.Builder multiTablesInputInfosBuilder = MultiTablesInputInfos.newBuilder();
        multiTablesInputInfosBuilder.setSplitCount(splitCount);
        multiTablesInputInfosBuilder.setSplitSize(splitSize);
        multiTablesInputInfosBuilder.setInputId(odpsRddId);
        if (splitTempDir != "") {
            multiTablesInputInfosBuilder.setSplitTempDir(splitTempDir);
        }

        MultiTablesInputInfoItem.Builder multiTablesInputInfoItemBuilder = MultiTablesInputInfoItem.newBuilder();
        multiTablesInputInfoItemBuilder.setProjName(project);
        multiTablesInputInfoItemBuilder.setTblName(table);
        String columnsStr = "";
        if (columns.length > 0) {
            columnsStr = columns[0];
            for (int i = 1; i < columns.length; i ++) {
                columnsStr += "," + columns[i];
            }
        }
        if (columnsStr != "") {
            logger.info("The user columnStr = " + columnsStr);
        }
        multiTablesInputInfoItemBuilder.setCols(columnsStr);
        if (partition.length > 0) {
            for (String partSpecTmp : partition) {
                multiTablesInputInfoItemBuilder.setPartSpecs(partSpecTmp);
                multiTablesInputInfosBuilder.addMultiTablesInputInfoItem(multiTablesInputInfoItemBuilder.build());
            }
        } else {
            multiTablesInputInfosBuilder.addMultiTablesInputInfoItem(multiTablesInputInfoItemBuilder.build());
        }

        CupidTaskParam.Builder cupidTaskParamBuilder = getPartitionSizeBaseInfo();
        cupidTaskParamBuilder.setMultiTablesInputInfos(multiTablesInputInfosBuilder.build());
        return SubmitJobUtil.getPartitionSizeSubmitJob(cupidTaskParamBuilder.build());
    }
}
