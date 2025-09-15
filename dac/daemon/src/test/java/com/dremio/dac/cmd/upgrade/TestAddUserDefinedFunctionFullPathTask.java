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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.ConnectionReaderImpl;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.NamespaceStore;
import com.dremio.service.namespace.function.proto.FunctionBody;
import com.dremio.service.namespace.function.proto.FunctionConfig;
import com.dremio.service.namespace.function.proto.FunctionDefinition;
import com.dremio.service.namespace.function.proto.ReturnType;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.test.DremioTest;
import com.google.common.collect.ImmutableList;
import io.protostuff.ByteString;
import java.util.Collections;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Test class for {@code AddUserDefinedFunctionFullPathTask} */
public class TestAddUserDefinedFunctionFullPathTask extends DremioTest {
  private static final String UDF_WITHOUT_FULL_PATH = "udf_without_full_path";
  private static final String UDF_WITH_FULL_PATH = "udf_with_full_path";

  private NamespaceStore namespaceStore;

  @Test
  public void testUpgrade() throws Exception {
    final LocalKVStoreProvider kvStoreProvider =
        new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
    final LegacyKVStoreProvider legacyKVStoreProvider = kvStoreProvider.asLegacy();
    final ScanResult scanResult = DremioTest.CLASSPATH_SCAN_RESULT;
    final LogicalPlanPersistence lpPersistence = new LogicalPlanPersistence(scanResult);
    final ConnectionReader connectionReader =
        spy(ConnectionReader.of(scanResult, ConnectionReaderImpl.class));

    kvStoreProvider.start();

    UpgradeContext upgradeContext =
        new UpgradeContext(
            kvStoreProvider,
            legacyKVStoreProvider,
            lpPersistence,
            connectionReader,
            DremioTest.DEFAULT_DREMIO_CONFIG,
            scanResult);
    namespaceStore = new NamespaceStore(() -> kvStoreProvider);

    // Create 2 UDFs, one with full path and one without
    createUDF(UDF_WITHOUT_FULL_PATH, false);
    createUDF(UDF_WITH_FULL_PATH, true);

    // Verify that one UDF has full path and the other does not
    assertThat(getUDF(UDF_WITHOUT_FULL_PATH).getFullPathList()).isNull();
    assertThat(getUDF(UDF_WITH_FULL_PATH).getFullPathList()).containsExactly(UDF_WITH_FULL_PATH);

    // Run the upgrade task
    final AddUserDefinedFunctionFullPathTask task = new AddUserDefinedFunctionFullPathTask();
    task.upgrade(upgradeContext);

    // Verify that both UDFs have full path
    assertThat(getUDF(UDF_WITHOUT_FULL_PATH).getFullPathList())
        .containsExactly(UDF_WITHOUT_FULL_PATH);
    assertThat(getUDF(UDF_WITH_FULL_PATH).getFullPathList()).containsExactly(UDF_WITH_FULL_PATH);
  }

  private void createUDF(String name, boolean withFullPath) {
    FunctionConfig functionConfig =
        new FunctionConfig()
            .setId(new EntityId(UUID.randomUUID().toString()))
            .setName(name)
            .setFunctionDefinitionsList(
                ImmutableList.of(
                    new FunctionDefinition()
                        .setFunctionBody(
                            new FunctionBody().setRawBody("SELECT 1").setSerializedPlan(null))
                        .setFunctionArgList(Collections.emptyList())))
            .setReturnType(
                new ReturnType().setRawDataType(ByteString.copyFrom(CompleteType.INT.serialize())));
    if (withFullPath) {
      functionConfig.setFullPathList(ImmutableList.of(name));
    }

    namespaceStore.put(
        NamespaceServiceImpl.getKey(new NamespaceKey(ImmutableList.of(name))),
        new NameSpaceContainer()
            .setType(NameSpaceContainer.Type.FUNCTION)
            .setFunction(functionConfig)
            .setFullPathList(ImmutableList.of(name)));
  }

  private FunctionConfig getUDF(String name) {
    return namespaceStore
        .get(NamespaceServiceImpl.getKey(new NamespaceKey(ImmutableList.of(name))))
        .getValue()
        .getFunction();
  }
}
