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

import com.github.rholder.retry.*;
import org.apache.log4j.Logger;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class RetryUtil {

    private static Logger logger = Logger.getLogger(RetryUtil.class);

    public static <T> T retryFunction(Callable<T> f, String message) throws ExecutionException, RetryException {
        return retryFunction(f, message, 0, 1);
    }

    public static <T> T retryFunction(Callable<T> f, String message, int RetryTimes) throws ExecutionException, RetryException {
        return retryFunction(f, message, RetryTimes, 1);
    }

    public static <T> T retryFunction(Callable<T> f, String message, int RetryTimes, int IntervalTimes) throws ExecutionException, RetryException {
        logger.debug("enter retryFunction with message: " + message + ", RetryTimes: " + RetryTimes + ", IntervalTimes: " + IntervalTimes);
        Retryer<T> retryer = RetryerBuilder.<T>newBuilder()
                .retryIfExceptionOfType(Throwable.class)
                .withWaitStrategy(WaitStrategies.fixedWait(IntervalTimes, TimeUnit.SECONDS))
                .withStopStrategy(StopStrategies.stopAfterAttempt(RetryTimes))
                .build();

        try {
            return retryer.call(f);
        } catch (RetryException e) {
            e.printStackTrace();
            throw e;
        } catch (ExecutionException e) {
            e.printStackTrace();
            throw e;
        }
    }
}
