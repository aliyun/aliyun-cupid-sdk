/*
 Author: Rongpeng Zheng(rongpeng.zrp@alibaba-inc.com)
 */

package com.aliyun.odps.cupid.requestcupid;

import apsara.odps.cupid.protocol.CupidTaskParamProtos;
import com.aliyun.odps.Instance;
import com.aliyun.odps.cupid.CupidSession;
import com.aliyun.odps.cupid.utils.SDKConstants;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

import static com.aliyun.odps.cupid.CupidUtil.getResult;

public class CreateK8sClusterUtil {
    private static final Logger LOG = Logger.getLogger(CreateK8sClusterUtil.class);

    private static final String DEFAULT_RUNNING_MORE = "longtime";

    /**
     * Create a kubernetes cluster
     *
     * @param conf         configurations to create the cluster
     *                     SDKConstants.DEFAULT_ISOLATION and SDKConstants.MASTER_TYPE will be set to
     *                     "false" and "kubernetes" anywise because they are required
     * @param cupidSession
     * @return Return Instance ID if succeed, otherwise null
     */
    public static String createCluster(HashMap<String, String> conf,
                                       CupidSession cupidSession) throws Exception {
        //Build cupidTaskOperator
        CupidTaskParamProtos.CupidTaskOperator.Builder cupidTaskOperator = CupidTaskParamProtos.CupidTaskOperator.newBuilder();
        cupidTaskOperator.setMoperator(CupidTaskOperatorConst.CUPID_TASK_START);
        cupidTaskOperator.setMlookupName("");

        String engineType = DEFAULT_RUNNING_MORE;//engineType: longtime by default
        if (conf.containsKey(SDKConstants.ENGINE_RUNNING_TYPE)) {
            engineType = conf.get(SDKConstants.ENGINE_RUNNING_TYPE);
        }
        cupidTaskOperator.setMenginetype(engineType);

        boolean isLongRunning = engineType.equals(DEFAULT_RUNNING_MORE);

        Integer priority = 1;
        if (conf.containsKey(SDKConstants.CUPID_JOB_PRIORITY)) {
            priority = Integer.parseInt(conf.get(SDKConstants.CUPID_JOB_PRIORITY));
        }

        conf.put(SDKConstants.DEFAULT_ISOLATION, "false");
        conf.put(SDKConstants.MASTER_TYPE, "kubernetes");


        //Build jobConf
        CupidTaskParamProtos.JobConf.Builder confBuilder = CupidTaskParamProtos.JobConf.newBuilder();
        for (Map.Entry<String, String> entry : conf.entrySet()) {
            if (entry.getKey().startsWith("odps.") || entry.getKey().startsWith("cupid.")) {
                CupidTaskParamProtos.JobConfItem.Builder itemBuilder = CupidTaskParamProtos.JobConfItem.newBuilder();
                itemBuilder.setKey(entry.getKey());
                itemBuilder.setValue(entry.getValue());
                confBuilder.addJobconfitem(itemBuilder);
            }
        }

        //Build CupidTaskParam: cupidTaskOperator and jobConf
        CupidTaskParamProtos.CupidTaskParam.Builder cupidTaskParamPB = CupidTaskParamProtos.CupidTaskParam.newBuilder();
        cupidTaskParamPB.setMcupidtaskoperator(cupidTaskOperator);
        cupidTaskParamPB.setJobconf(confBuilder);

        Instance instance;
        try {
            //send a request and get a instance
            instance = SubmitJobUtil.submitJob(cupidTaskParamPB.build(), "eHasFuxiJob", priority, isLongRunning, cupidSession);
        } catch (Exception e) {
            LOG.error("Getting instance failed. ",e);
            throw e;
        }

        if (instance != null) {
            LOG.info("Instance ID: " + instance.getId());
        }

        //Request result
        CupidTaskParamProtos.CupidTaskDetailResultParam res;
        try {
            res = getResult(instance);
        } catch (Exception e) {
            LOG.error("Getting result failed. ",e);
            return null;
        }
        if (res.getRunning() != null) { //succeed
            return instance.getId();
        } else if (res.getFailed() != null) {
            throw new Exception("create odps cluster fail, error message is: " + res.getFailed().getBizFailed().getBizFailedMsg());
        } else if (res.getCancelled() != null) {
            throw new Exception("create odps cluster fail, error message is: instance was canceled. ");
        } else if (res.getSuccess() != null) {
            throw new Exception("create odps cluster fail, error message is: " + res.getSuccess().getSuccessMsg());
        } else {
            throw new Exception("create odps cluster fail, error message is: unexpected status. ");
        }
    }
}
