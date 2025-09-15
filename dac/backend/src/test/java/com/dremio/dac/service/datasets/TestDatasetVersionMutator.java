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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.ProtostuffUtil;
import com.dremio.dac.explore.QueryParser;
import com.dremio.dac.explore.model.DatasetName;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.sources.SourceName;
import com.dremio.dac.proto.model.dataset.DatasetVersionOrigin;
import com.dremio.dac.proto.model.dataset.NameDatasetRef;
import com.dremio.dac.proto.model.dataset.VirtualDatasetState;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.proto.model.dataset.VirtualDatasetVersion;
import com.dremio.dac.util.DatasetsUtil;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.iceberg.SupportsIcebergRestApi;
import com.dremio.options.OptionManager;
import com.dremio.plugins.dataplane.store.DataplanePlugin;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TestDatasetVersionMutator {
  @Mock private OptionManager optionManager;
  @Mock private Catalog catalog;
  @Mock private CatalogService catalogService;
  @Mock private LegacyKVStoreProvider legacyKVStoreProvider;
  @Mock private JobsService jobsService;
  @Mock private SabotContext sabotContext;
  @Mock private DataplanePlugin dataplanePlugin;

  @Mock
  private LegacyKVStore<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>
      datasetVersions;

  @Mock private StoragePlugin sourcePlugin;

  private static final String versionedSourceName = "nessie";

  private DatasetVersionMutator datasetVersionMutator;

  @BeforeEach
  public void setup() throws Exception {
    when(legacyKVStoreProvider.getStore(any())).thenReturn(datasetVersions);

    datasetVersionMutator =
        new DatasetVersionMutator(
            legacyKVStoreProvider, jobsService, catalogService, optionManager, sabotContext);
  }

  @Test
  public void testRenameDatasetForVersionedSource() throws Exception {
    setupForVersionedSource();
    DatasetPath oldDatasetPath =
        new DatasetPath(new SourceName(versionedSourceName), new DatasetName("testTable"));
    DatasetPath newDatasetPath =
        new DatasetPath(new SourceName(versionedSourceName), new DatasetName("testMoveTable"));

    assertThatThrownBy(() -> datasetVersionMutator.renameDataset(oldDatasetPath, newDatasetPath))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("not allowed in Versioned source");
  }

  @Test
  public void testCopyFromDatasetForVersionedSource() throws Exception {
    when(catalogService.getSystemUserCatalog()).thenReturn(catalog);

    DatasetPath datasetPath = new DatasetPath(new SourceName("test"), new DatasetName("testTable"));

    assertThatThrownBy(
            () -> datasetVersionMutator.createDatasetFrom(datasetPath, datasetPath, "userName"))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("already exists");
  }

  @Test
  public void testCopyFromDatasetForTheSamePath() throws Exception {
    setupForVersionedSource();
    DatasetPath datasetPath =
        new DatasetPath(new SourceName(versionedSourceName), new DatasetName("testTable"));

    assertThatThrownBy(
            () -> datasetVersionMutator.createDatasetFrom(datasetPath, datasetPath, "userName"))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("not allowed within Versioned source");
  }

  @Test
  public void testPutVersion_createdTimeSet() {
    VirtualDatasetUI uiProto =
        new VirtualDatasetUI()
            .setId("123")
            .setFullPathList(ImmutableList.of("path"))
            .setSqlFieldsList(ImmutableList.of(new ViewFieldType().setName("a").setType("int")))
            .setState(new VirtualDatasetState())
            .setVersion(new DatasetVersion("1"))
            .setCreatedAt(123L);

    datasetVersionMutator.putVersion(uiProto);
    DatasetVersionMutator.VersionDatasetKey key =
        new DatasetVersionMutator.VersionDatasetKey(
            new DatasetPath(uiProto.getFullPathList()), uiProto.getVersion());
    verify(datasetVersions, times(1))
        .put(eq(key), argThat((arg) -> arg.getDataset().getCreatedAt() != 123L));
  }

  @Test
  public void testPutVersion_createdTimeNotSet() {
    VirtualDatasetUI uiProto =
        new VirtualDatasetUI()
            .setId("123")
            .setFullPathList(ImmutableList.of("path"))
            .setSqlFieldsList(ImmutableList.of(new ViewFieldType().setName("a").setType("int")))
            .setState(new VirtualDatasetState())
            .setVersion(new DatasetVersion("1"));
    VirtualDatasetUI baseUiProto = new VirtualDatasetUI().setCreatedAt(123L);
    VirtualDatasetVersion storageProto =
        DatasetsUtil.toVirtualDatasetVersion(
            ProtostuffUtil.copy(uiProto).setCreatedAt(baseUiProto.getCreatedAt()));
    datasetVersionMutator.putVersion(uiProto, baseUiProto);
    DatasetVersionMutator.VersionDatasetKey key =
        new DatasetVersionMutator.VersionDatasetKey(
            new DatasetPath(uiProto.getFullPathList()), uiProto.getVersion());
    verify(datasetVersions, times(1)).put(eq(key), eq(storageProto));
    assertEquals(uiProto.getCreatedAt(), storageProto.getDataset().getCreatedAt());
  }

  static Stream<Arguments> queriesWithExpectedFailure() {
    return Stream.of(
        Arguments.of("SELECT 1 AS col, 2 AS col", List.of("col", "col"), true),
        Arguments.of(
            "SELECT a.x, b.x FROM (SELECT 1 AS x) a, (SELECT 2 AS x) b", List.of("x", "x"), true),
        Arguments.of("SELECT 1 AS col1, 2 AS col2", List.of("col1", "col2"), false));
  }

  @ParameterizedTest
  @MethodSource("queriesWithExpectedFailure")
  public void testPutRejectsOrAcceptsBasedOnDuplicateColumns(
      String sql, List<String> colNames, boolean shouldFail) {
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

    RelDataType rowType = typeFactory.createStructType(List.of(intType, intType), colNames);

    ConvertedRelNode mockRelNode = mock(ConvertedRelNode.class);
    when(mockRelNode.getValidatedRowType()).thenReturn(rowType);

    try (MockedStatic<QueryParser> queryParserMock = mockStatic(QueryParser.class)) {
      queryParserMock
          .when(() -> QueryParser.validateVersions(any(), any(), any()))
          .thenReturn(mockRelNode);

      VirtualDatasetUI uiProto =
          new VirtualDatasetUI()
              .setId("1")
              .setFullPathList(ImmutableList.of("myspace", "query_" + colNames.get(0)))
              .setSql(sql)
              .setSqlFieldsList(
                  colNames.stream()
                      .map(name -> new ViewFieldType().setName(name).setType("INTEGER"))
                      .collect(ImmutableList.toImmutableList()))
              .setState(new VirtualDatasetState())
              .setVersion(new DatasetVersion("1"));

      if (shouldFail) {
        assertThatThrownBy(() -> datasetVersionMutator.put(uiProto))
            .isInstanceOf(UserException.class)
            .hasMessageContaining("Duplicate column name '" + colNames.get(0) + "'");
      } else {
        when(catalogService.getSystemUserCatalog()).thenReturn(mock(Catalog.class));
        assertThatCode(() -> datasetVersionMutator.put(uiProto)).doesNotThrowAnyException();
      }
    }
  }

  @Test
  public void testGettingCorrectSavedVersion() throws NamespaceNotFoundException {
    List<String> path = ImmutableList.of("path");
    VirtualDatasetUI first =
        new VirtualDatasetUI()
            .setId("1")
            .setFullPathList(path)
            .setSqlFieldsList(ImmutableList.of(new ViewFieldType().setName("a").setType("int")))
            .setState(new VirtualDatasetState())
            .setVersion(new DatasetVersion("1"));

    VirtualDatasetUI second =
        new VirtualDatasetUI()
            .setId("2")
            .setFullPathList(path)
            .setSqlFieldsList(ImmutableList.of(new ViewFieldType().setName("a").setType("int")))
            .setState(new VirtualDatasetState())
            .setVersion(new DatasetVersion("2"))
            .setDatasetVersionOrigin(DatasetVersionOrigin.SAVE);

    VirtualDatasetUI third =
        new VirtualDatasetUI()
            .setId("3")
            .setFullPathList(path)
            .setSqlFieldsList(ImmutableList.of(new ViewFieldType().setName("a").setType("int")))
            .setState(new VirtualDatasetState())
            .setVersion(new DatasetVersion("3"));

    NameDatasetRef firstRef =
        new NameDatasetRef()
            .setDatasetPath("path")
            .setDatasetVersion(first.getVersion().getVersion());
    second.setPreviousVersion(firstRef);

    NameDatasetRef secondRef =
        new NameDatasetRef()
            .setDatasetPath("path")
            .setDatasetVersion(second.getVersion().getVersion());
    third.setPreviousVersion(secondRef);

    when(catalogService.getSystemUserCatalog()).thenReturn(catalog);
    when(catalog.getSource(Mockito.<String>any())).thenReturn(sourcePlugin);
    when(sourcePlugin.isWrapperFor(SupportsIcebergRestApi.class)).thenReturn(false);

    datasetVersionMutator.putVersion(first);
    datasetVersionMutator.putVersion(second);
    datasetVersionMutator.putVersion(third);

    when(datasetVersions.get(
            new DatasetVersionMutator.VersionDatasetKey(
                new DatasetPath(path), second.getVersion())))
        .thenReturn(
            new VirtualDatasetVersion()
                .setPreviousVersion(firstRef)
                .setDatasetVersionOrigin(DatasetVersionOrigin.SAVE));

    when(datasetVersions.get(
            new DatasetVersionMutator.VersionDatasetKey(new DatasetPath(path), third.getVersion())))
        .thenReturn(new VirtualDatasetVersion().setPreviousVersion(secondRef));

    // the version history looks like third -> second (saved) -> first. Therefore, when we are
    // getting
    // saved version from the third, we will stop at second version and return it.
    assertEquals(
        datasetVersionMutator.getLatestVersionByOrigin(
            new DatasetPath(path), third.getVersion(), DatasetVersionOrigin.SAVE),
        second.getVersion());
  }

  @Test
  public void testGetLatestVersionByOriginBrokenHistory() throws NamespaceNotFoundException {
    List<String> path = ImmutableList.of("path");
    VirtualDatasetUI first =
        new VirtualDatasetUI()
            .setId("1")
            .setFullPathList(path)
            .setSqlFieldsList(ImmutableList.of(new ViewFieldType().setName("a").setType("int")))
            .setState(new VirtualDatasetState())
            .setVersion(new DatasetVersion("1"))
            .setDatasetVersionOrigin(DatasetVersionOrigin.SAVE);

    VirtualDatasetUI second =
        new VirtualDatasetUI()
            .setId("2")
            .setFullPathList(path)
            .setSqlFieldsList(ImmutableList.of(new ViewFieldType().setName("a").setType("int")))
            .setState(new VirtualDatasetState())
            .setVersion(new DatasetVersion("2"))
            .setDatasetVersionOrigin(DatasetVersionOrigin.RUN);

    VirtualDatasetUI third =
        new VirtualDatasetUI()
            .setId("3")
            .setFullPathList(path)
            .setSqlFieldsList(ImmutableList.of(new ViewFieldType().setName("a").setType("int")))
            .setState(new VirtualDatasetState())
            .setVersion(new DatasetVersion("3"))
            .setDatasetVersionOrigin(DatasetVersionOrigin.RUN);

    NameDatasetRef firstRef =
        new NameDatasetRef()
            .setDatasetPath("path")
            .setDatasetVersion(first.getVersion().getVersion());
    second.setPreviousVersion(firstRef);

    NameDatasetRef secondRef =
        new NameDatasetRef()
            .setDatasetPath("path")
            .setDatasetVersion(second.getVersion().getVersion());
    third.setPreviousVersion(secondRef);

    when(catalogService.getSystemUserCatalog()).thenReturn(catalog);
    when(catalog.getSource(Mockito.<String>any())).thenReturn(sourcePlugin);
    when(sourcePlugin.isWrapperFor(SupportsIcebergRestApi.class)).thenReturn(false);

    // Don't put first version to validate behavior when it's missing
    datasetVersionMutator.putVersion(second);
    datasetVersionMutator.putVersion(third);

    when(datasetVersions.get(
            new DatasetVersionMutator.VersionDatasetKey(new DatasetPath(path), first.getVersion())))
        .thenReturn(null);

    when(datasetVersions.get(
            new DatasetVersionMutator.VersionDatasetKey(
                new DatasetPath(path), second.getVersion())))
        .thenReturn(DatasetsUtil.toVirtualDatasetVersion(second));

    when(datasetVersions.get(
            new DatasetVersionMutator.VersionDatasetKey(new DatasetPath(path), third.getVersion())))
        .thenReturn(DatasetsUtil.toVirtualDatasetVersion(third));

    // the version history looks like third (run) -> second (run) -> first, with first missing from
    // the dataset version store. Therefore, we will return null since no saved origin will be found
    assertNull(
        datasetVersionMutator.getLatestVersionByOrigin(
            new DatasetPath(path), third.getVersion(), DatasetVersionOrigin.SAVE));
  }

  @Test
  public void testGetVersionForSingleVersionReturnsNull() {
    List<String> path = ImmutableList.of("path");
    VirtualDatasetUI first =
        new VirtualDatasetUI()
            .setId("1")
            .setFullPathList(path)
            .setSqlFieldsList(ImmutableList.of(new ViewFieldType().setName("a").setType("int")))
            .setState(new VirtualDatasetState())
            .setVersion(new DatasetVersion("1"));

    when(catalogService.getSystemUserCatalog()).thenReturn(catalog);
    when(catalog.getSource(Mockito.<String>any())).thenReturn(sourcePlugin);
    when(sourcePlugin.isWrapperFor(SupportsIcebergRestApi.class)).thenReturn(false);

    datasetVersionMutator.putVersion(first);

    when(datasetVersions.get(
            new DatasetVersionMutator.VersionDatasetKey(new DatasetPath(path), first.getVersion())))
        .thenReturn(new VirtualDatasetVersion());

    assertNull(
        datasetVersionMutator.getLatestVersionByOrigin(
            new DatasetPath(path), first.getVersion(), DatasetVersionOrigin.SAVE));
  }

  @Test
  public void testGetVersionFromCatalogForIcebergRestApiSources() throws Exception {
    when(catalogService.getSystemUserCatalog()).thenReturn(catalog);

    DatasetPath path = new DatasetPath(ImmutableList.of("iceberg", "db", "table"));
    DatasetVersion version = new DatasetVersion("1");
    NamespaceKey namespaceKey = new NamespaceKey(path.toPathList());

    when(catalog.getSource("iceberg")).thenReturn(sourcePlugin);
    when(sourcePlugin.isWrapperFor(SupportsIcebergRestApi.class)).thenReturn(true);

    ViewTable viewTable = mock(ViewTable.class);
    String sql = "SELECT * FROM sourceTable";
    List<String> sqlContextList = ImmutableList.of("context1", "context2");
    DatasetConfig expectedConfig =
        new DatasetConfig()
            .setType(DatasetType.VIRTUAL_DATASET)
            .setId(new EntityId().setId("test-id"))
            .setFullPathList(namespaceKey.getPathComponents())
            .setVirtualDataset(
                new VirtualDataset()
                    .setSql(sql)
                    .setVersion(version)
                    .setContextList(sqlContextList));

    when(viewTable.getDatasetConfig()).thenReturn(expectedConfig);
    when(catalog.getDataset(namespaceKey)).thenReturn(expectedConfig);
    when(catalog.getTable(namespaceKey)).thenReturn(viewTable);

    VirtualDatasetUI result = datasetVersionMutator.getVersion(path, version);

    assertThat(result).isNotNull();
    assertThat(result.getSql()).isEqualTo(sql);
    assertThat(result.getVersion()).isEqualTo(version);
    verify(datasetVersions, times(1)).get(Mockito.<DatasetVersionMutator.VersionDatasetKey>any());
    verify(catalog).getTable(namespaceKey);
  }

  private void setupForVersionedSource() throws NamespaceException {
    NameSpaceContainer nameSpaceContainer = mock(NameSpaceContainer.class);
    when(nameSpaceContainer.getType()).thenReturn(NameSpaceContainer.Type.SOURCE);
    when(catalog.getEntityByPath(eq(new NamespaceKey(versionedSourceName))))
        .thenReturn(nameSpaceContainer);
    when(catalogService.getSystemUserCatalog()).thenReturn(catalog);
    when(catalogService.getSource(versionedSourceName)).thenReturn(dataplanePlugin);
    when(dataplanePlugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
  }
}
