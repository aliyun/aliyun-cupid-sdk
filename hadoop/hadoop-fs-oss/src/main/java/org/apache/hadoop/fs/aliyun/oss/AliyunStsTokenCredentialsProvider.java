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

import com.aliyun.oss.common.auth.Credentials;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.DefaultCredentials;
import com.aliyun.oss.common.auth.InvalidCredentialsException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

import static org.apache.hadoop.fs.aliyun.oss.Constants.*;

/**
 * Support session credentials for authenticating with Aliyun.
 */
public class AliyunStsTokenCredentialsProvider implements CredentialsProvider
{
  private Credentials credentials = null;

  public AliyunStsTokenCredentialsProvider(Configuration conf)
      throws IOException
  {
    String accessKeyId;
    String accessKeySecret;
    String securityToken;
    try {
      accessKeyId = AliyunOSSUtils.getValueWithKey(conf, ACCESS_KEY_ID);
      accessKeySecret = AliyunOSSUtils.getValueWithKey(conf, ACCESS_KEY_SECRET);
    } catch (IOException e) {
      throw new InvalidCredentialsException(e);
    }

    try {
      securityToken = AliyunOSSUtils.getValueWithKey(conf, SECURITY_TOKEN);
    } catch (IOException e) {
      securityToken = null;
    }
  }

  @Override
  public void setCredentials(Credentials creds)
  {
    if (creds == null) {
      throw new InvalidCredentialsException("Credentials should not be null.");
    }

    credentials = creds;
  }

  @Override
  public Credentials getCredentials()
  {
    if (credentials == null) {
      throw new InvalidCredentialsException("Invalid credentials");
    }

    return credentials;
  }
}
