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
package com.dremio.dac.cmd.upgrade;

import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.FUNCTION;

import com.dremio.dac.cmd.AdminLogger;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.ImmutableFindByCondition;
import com.dremio.service.namespace.NamespaceIndexKeys;
import com.dremio.service.namespace.NamespaceStore;
import com.dremio.service.namespace.function.proto.FunctionConfig;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import java.util.Collections;

/**
 * Since 22.3.0, fullPath is explicitly set for UDFs. UDFs that were created before 22.3.0 but were
 * never updated since then do not have fullPath set. This task adds the fullPath for such UDFs.
 */
public class AddUserDefinedFunctionFullPathTask extends UpgradeTask {

  // DO NOT MODIFY
  static final String taskUUID = "6DD59F78-F8E7-40E0-B47A-358C286DFFC8";

  public AddUserDefinedFunctionFullPathTask() {
    super("Add full path to UDFs which don't have it", Collections.emptyList());
  }

  @Override
  public String getTaskUUID() {
    return taskUUID;
  }

  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    AdminLogger.log("Starting upgrade task to add full path to UDFs.");

    var namespaceStore =
        context.getKvStoreProvider().getStore(NamespaceStore.NamespaceStoreCreator.class);

    final Iterable<Document<String, NameSpaceContainer>> udfDocs =
        namespaceStore.find(
            new ImmutableFindByCondition.Builder()
                .setCondition(
                    SearchQueryUtils.newTermQuery(
                        NamespaceIndexKeys.ENTITY_TYPE.getIndexFieldName(), FUNCTION.getNumber()))
                .build());

    int totalCount = 0;
    int updatedCount = 0;
    for (Document<String, NameSpaceContainer> doc : udfDocs) {
      totalCount++;
      NameSpaceContainer container = doc.getValue();
      FunctionConfig functionConfig = container.getFunction();
      if (functionConfig.getFullPathList() == null) {
        updatedCount++;
        AdminLogger.log("UDF {}:\n{}", container.getFullPathList(), functionConfig.toString());

        functionConfig.setFullPathList(container.getFullPathList());
        namespaceStore.put(doc.getKey(), container);
      }
    }

    AdminLogger.log("Upgrade task finished. Updated {} out of {} UDFs", updatedCount, totalCount);
  }
}
