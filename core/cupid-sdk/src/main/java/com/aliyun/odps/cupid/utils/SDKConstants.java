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

package com.aliyun.odps.cupid.utils;

/**
 * Created by tianli on 16/8/1.
 */
public class SDKConstants {
  public final static String ENGINE_RUNNING_TYPE = "odps.cupid.engine.running.type";
  public final static String ENGINE_TYPE = "engine.type";
  public final static String INTERACTION = "interaction";
  public final static String MASTER = "master";
  public final static String CUPID_RUNNING_MODE = "odps.cupid.running.mode";
  public final static String CUPID_JOB_PRIORITY = "odps.cupid.job.priority";


  /**
   * Create new kubernetes cluster
   */
  public final static String DEFAULT_ISOLATION = "odps.cupid.isolation.default";
  public final static String MASTER_TYPE = "odps.cupid.master.type";

  /**
   * INTERACTION
   */
  public final static String CUPID_INTERACTION_PROXY_ENDPOINT =
      "odps.cupid.interaction.proxy.endpoint";
  public final static String CUPID_INTERACTION_PROXY_TOKEN = "odps.cupid.interaction.proxy.token";
  public final static String CUPID_INTERACTION_PROXY_ROUTE_INFO =
      "odps.cupid.interaction.proxy.route.info";

  public final static String CUPID_INTERACTION_PROTOCOL = "odps.cupid.interaction.protocol";
  public final static String CUPID_INTERACTION_SUB_PROTOCOL_APP = "interaction_app";
  public final static String CUPID_INTERACTION_SUB_PROTOCOL_CLIENT = "interaction_client";
  public final static String CUPID_INTERACTION_WEBSOCKET_SCHEMA = "ws://";
  public final static String CUPID_INTERACTION_WEBSOCKET_PATH = "/interaction/";


  public final static String CUPID_INTERACTION_RETRY_COUNT_ON_ERROR =
      "odps.cupid.interaction.retry.count";
  public final static String CUPID_INTERACTION_RETRY_INTERVAL_ON_ERROR =
      "odps.cupid.interaction.retry.interval";

  public final static String CUPID_INTERACTION_HEADER_RECONNECT = "odps-cupid-proxy-reconnect";
  public final static String CUPID_INTERACTION_HEADER_TOKEN = "odps-cupid-proxy-token";
  public final static String CUPID_INTERACTION_HEADER_SET_COOKIE = "Set-Cookie";
  public final static String CUPID_INTERACTION_HEADER_ROUTE_INFO = "odps-cupid-proxy-route-info";
  public final static String CUPID_INTERACTION_COOKIE_HASH_KEY = "route";

  public final static String ENV_CUPID_RUNNING_MODE = "CUPID_RUNNING_MODE";
  public final static String ENV_CUPID_ROLE = "CUPID_ROLE";
  public final static String ENV_CUPID_INTERACTION_PROXY_ROUTE_INFO =
      "CUPID_INTERACTION_PROXY_ROUTE_INFO";
  public final static String ENV_CUPID_INTERACTION_PROXY_TOKEN = "CUPID_INTERACTION_PROXY_TOKEN";
  public final static String ENV_CUPID_INTERACTION_PROTOCOL = "CUPID_INTERACTION_PROTOCOL";

  public final static int INTERACTION_CLIENT_INPUT_MODE_UNDEFINED = -1;
  public final static int INTERACTION_CLIENT_INPUT_MODE_FD = 0;
  public final static int INTERACTION_CLIENT_INPUT_MODE_INPUTSTREAM = 1;

  public final static int WEBSOCKET_MAX_BINARY_MESSAGE_SIZE = 64 * 1024;

  public final static int ONE_HOUR = 60 * 60 * 1000;

  public final static String DEFAULT_CHARSET = "UTF-8";

  public final static String WEBSOCKET_STATUS_MESSAGE_SHUTDOWN = "Shutdown";


}
