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

import com.dremio.dac.cmd.AdminLogger;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.datastore.api.ImmutableFindByCondition;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.service.scripts.ScriptStoreImpl;
import com.dremio.service.scripts.proto.ScriptProto;
import java.util.Collections;

/**
 * Re-indexes the Script Store because a new key was added that requires the name to be stored in
 * lowercase to then be used as searching Scripts case-insensitively.
 */
public class ReIndexScriptStore extends UpgradeTask {
  // DO NOT MODIFY
  static final String taskUUID = "8fb0b5ae-0b14-47bf-9167-56a13f5b2f6a";

  public ReIndexScriptStore() {
    super("Re-index Script Store.", Collections.emptyList());
  }

  @Override
  public String getTaskUUID() {
    return taskUUID;
  }

  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    IndexedStore<String, ScriptProto.Script> scriptStore =
        context.getKvStoreProvider().getStore(ScriptStoreImpl.StoreCreator.class);

    SearchTypes.SearchQuery getAllScriptsCondition = SearchQueryUtils.newMatchAllQuery();
    FindByCondition findAllScriptsCondition =
        new ImmutableFindByCondition.Builder()
            .setCondition(getAllScriptsCondition)
            .setPageSize(100)
            .build();

    long reindexedCount = scriptStore.reindex(findAllScriptsCondition);
    AdminLogger.log("Re-indexed {} documents in the Script Store.", reindexedCount);
  }
}
