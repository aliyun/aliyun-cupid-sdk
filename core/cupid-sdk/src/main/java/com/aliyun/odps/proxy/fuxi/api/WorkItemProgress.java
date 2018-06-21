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

package com.aliyun.odps.proxy.fuxi.api;

public enum WorkItemProgress {

  WIP_WAITING(1), //
  WIP_READY(2), // resource ready, but not running yet
  WIP_RUNNING(3), //
  WIP_TERMINATING(4), // worker has tried to terminate the
  // work item but is not yet confirmed
  WIP_TERMINATED(5), // work confirms that the work item is terminated
  // normally
  WIP_FAILED(6), // worker confirms that the work item has failed
  WIP_INTERRUPTED(7), //
  WIP_DEAD(8), //
  WIP_INTERRUPTING(9), // when inst pvc fail and backuppeer running,change
  // status to WIP_INTERRUPTING,when backuppeer
  // fail,change orig to failed
  // ====== Check TaskMaster::GetState() in task_master.cpp if you modify this
  // !!
  WIP_ENUM_END(10); // only used as end of iterator.

  private int value;

  WorkItemProgress(int value) {
    this.value = value;
  }

  public int value() {
    return this.value;
  }
}
