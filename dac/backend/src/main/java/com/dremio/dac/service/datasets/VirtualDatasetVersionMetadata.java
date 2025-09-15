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
package com.dremio.dac.service.datasets;

import com.dremio.dac.proto.model.dataset.NameDatasetRef;
import com.dremio.dac.proto.model.dataset.VirtualDatasetVersion;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * Holds a small subset of fields from {@link
 * com.dremio.dac.proto.model.dataset.VirtualDatasetVersion}.
 */
@Value.Immutable
public interface VirtualDatasetVersionMetadata {
  @Nullable
  Long getLastModified();

  @Nullable
  Long getCreatedAt();

  @Nullable
  NameDatasetRef getPreviousVersion();

  static VirtualDatasetVersionMetadata fromVersion(VirtualDatasetVersion version) {
    return new ImmutableVirtualDatasetVersionMetadata.Builder()
        .setLastModified(version.getDataset().getLastModified())
        .setCreatedAt(version.getDataset().getCreatedAt())
        .setPreviousVersion(version.getPreviousVersion())
        .build();
  }
}
