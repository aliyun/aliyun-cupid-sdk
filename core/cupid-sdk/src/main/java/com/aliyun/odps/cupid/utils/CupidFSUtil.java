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

import apsara.odps.cupid.protocol.VolumePathsCapProtos.VolumeCap;
import apsara.odps.cupid.protocol.VolumePathsCapProtos.VolumePaths;
import apsara.odps.cupid.protocol.VolumePathsCapProtos.PathInfo;
import com.aliyun.odps.cupid.CupidSession;
import org.apache.commons.codec.binary.Base64;

import java.util.Map;

/**
 * Created by tianli on 16/7/27.
 */
public class CupidFSUtil {

  public final static String VOLUME_PATH_INFO_KEY = "odps.moye.volume.path.info";

  public static void registerPathToCupidSession(Map<String, VolumeCap> infoMap) {
    VolumePaths.Builder volumePathsBuilder = VolumePaths.newBuilder();
    PathInfo.Builder pathInfoBuilder = PathInfo.newBuilder();

    for (Map.Entry<String, VolumeCap> infoEntry
        : infoMap.entrySet()) {
      pathInfoBuilder.setVolumepath(infoEntry.getKey());
      pathInfoBuilder.setVolumecap(infoEntry.getValue());
      volumePathsBuilder.addPathinfo(pathInfoBuilder.build());
    }

    CupidSession.get().conf.set(VOLUME_PATH_INFO_KEY,
        Base64.encodeBase64String(volumePathsBuilder.build().toByteArray()));
  }

}
