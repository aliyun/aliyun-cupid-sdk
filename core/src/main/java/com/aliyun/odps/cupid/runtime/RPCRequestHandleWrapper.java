package com.aliyun.odps.cupid.runtime;

import java.io.IOException;

/**
 * Created by liwei.li on 6/14/16.
 */
public interface RPCRequestHandleWrapper {
    byte[] handleRequest(byte[] rpcRequest) throws IOException;
    String getMethod();
}
