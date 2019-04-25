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

package org.apache.hadoop.fs.aliyun.oss;

import com.aliyun.oss.OSSException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

import java.io.IOException;

/**
 * Created by linxuewei on 2017/5/26.
 */
@Aspect
public final class RetryOnceSecurityTokenExpired
{

  private static final Log LOG = LogFactory.getLog(org.apache.hadoop.fs.aliyun.oss.RetryOnceSecurityTokenExpired.class);

  @Around("execution(public * org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem.*(..))")
  public Object wrap(final ProceedingJoinPoint point) throws Throwable
  {

    // AliyunOSSFileSystem instance
    AliyunOSSFileSystem target = (AliyunOSSFileSystem) point.getTarget();

    // hard-code for retry once
    int attempt = 0;
    int maxAttempt = 1;

    while (true) {
      try {
        LOG.debug(point.getSignature().getName() + " called, RetryOnceSecurityTokenExpired intercepted");
        return point.proceed();
      } catch (Throwable ex) {
        if (attempt >= maxAttempt) {
          LOG.warn(point.getSignature().getName() + " failed with " + ex.getMessage() + ", will not retry becaulse maxAttempt exceed");
          throw ex;
        }

        // in this case, NUWA StsToken has been replace and transient issue might happen
        if (ex.getClass() == OSSException.class && ((OSSException) ex).getErrorCode().equals("SecurityTokenExpired")) {
          LOG.warn(point.getSignature().getName() + " failed with SecurityTokenExpired, try initialize again and retry");
          target.initialize(target.getUri(), target.getConf());
        } else {
          LOG.warn(point.getSignature().getName() + " failed with " + ex.getMessage() + ", will not retry");
          throw ex;
        }
        ++attempt;
      }
    }
  }
}
