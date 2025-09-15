/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.connector.metadata.options;

import com.dremio.connector.metadata.GetDatasetOption;
import java.util.stream.Stream;

/**
 * Option to request force update from plugin during a dataset retrieval. Useful if plugin maintains
 * a dataset cache.
 *
 * <p>2 modes supported:
 *
 * <ul>
 *   <li>ALWAYS - will always request force update
 *   <li>ONCE - requests force update once per this ForceUpdateOption, useful e.g. if dataset
 *       handler retrieval method is wrapped in a supplier for multiple invocations, but we only
 *       need the first call ensured to be given fresh data
 * </ul>
 */
public class ForceUpdateOption implements GetDatasetOption {

  public enum Mode {
    ALWAYS,
    ONCE
  }

  private boolean isForceUpdate = true;
  private final Mode mode;

  public ForceUpdateOption(Mode mode) {
    this.mode = mode;
  }

  public boolean isForceUpdate() {
    if (isForceUpdate && mode == Mode.ONCE) {
      this.isForceUpdate = false;
      return true;
    } else {
      return isForceUpdate;
    }
  }

  public static boolean isForceUpdate(GetDatasetOption... options) {
    ForceUpdateOption option =
        (ForceUpdateOption)
            Stream.of(options).filter(o -> o instanceof ForceUpdateOption).findFirst().orElse(null);
    return option != null && option.isForceUpdate();
  }
}
