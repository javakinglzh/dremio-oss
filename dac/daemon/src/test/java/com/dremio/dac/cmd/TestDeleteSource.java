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
package com.dremio.dac.cmd;

import static com.dremio.test.DremioTest.CLASSPATH_SCAN_RESULT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.exec.catalog.CatalogSourceDataCreator;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.NamespaceStore;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.SourceInternalData;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestDeleteSource {

  private static LocalKVStoreProvider kvStoreProvider;
  private static NamespaceStore namespaceStore;

  @SuppressWarnings("deprecation") // Production code uses legacy here still, best to match in tests
  private static LegacyKVStore<NamespaceKey, SourceInternalData> sourceDataStore;

  @BeforeAll
  public static void beforeAll() throws Exception {
    kvStoreProvider = new LocalKVStoreProvider(CLASSPATH_SCAN_RESULT, null, true, false);
    kvStoreProvider.start();
    namespaceStore = new NamespaceStore(() -> kvStoreProvider);
    // Production code uses legacy here still, best to match in tests
    //noinspection deprecation
    sourceDataStore = kvStoreProvider.asLegacy().getStore(CatalogSourceDataCreator.class);
  }

  @BeforeEach
  public void beforeEach() throws IOException {
    kvStoreProvider.deleteEverything();
  }

  @AfterAll
  public static void afterAll() throws Exception {
    kvStoreProvider.close();
  }

  @Test
  public void testDeleteSourceWithoutChildren() throws Exception {
    final String source1Name = "source1";
    final List<String> source1Path = List.of(source1Name);
    createSource(source1Name);
    final String source2Name = "source2";
    final List<String> source2Path = List.of(source2Name);
    Document<String, NameSpaceContainer> source2Document = createSource(source2Name);

    DeleteSource.deleteSource(
        DeleteSource.Options.parse(new String[] {"--name", String.format("%s", source1Name)}),
        kvStoreProvider);

    assertThat(namespaceStore.get(keyFor(source1Path))).isNull();
    assertThat(namespaceStore.get(keyFor(source2Path))).isEqualTo(source2Document);
  }

  /**
   *
   *
   * <pre>
   * source1
   *  ├── dataset1
   *  └── folder1
   *      └── dataset2
   * source2
   * </pre>
   */
  @Test
  public void testDeleteSourceWithChildren() throws Exception {
    final String source1Name = "source1";
    final List<String> source1Path = List.of(source1Name);
    createSource(source1Name);
    final String dataset1Name = "dataset1";
    final List<String> dataset1Path = List.of(source1Name, dataset1Name);
    createDataset(dataset1Path);
    final String folderName = "folder1";
    final List<String> folderPath = List.of(source1Name, folderName);
    createFolder(folderPath);
    final String dataset2Name = "dataset2";
    final List<String> dataset2Path = List.of(source1Name, folderName, dataset2Name);
    createDataset(dataset2Path);
    final String source2Name = "source2";
    final List<String> source2Path = List.of(source2Name);
    Document<String, NameSpaceContainer> source2Document = createSource(source2Name);

    DeleteSource.deleteSource(
        DeleteSource.Options.parse(new String[] {"--name", String.format("%s", source1Name)}),
        kvStoreProvider);

    assertThat(namespaceStore.get(keyFor(source1Path))).isNull();
    assertThat(namespaceStore.get(keyFor(dataset1Path))).isNull();
    assertThat(namespaceStore.get(keyFor(folderPath))).isNull();
    assertThat(namespaceStore.get(keyFor(dataset2Path))).isNull();
    assertThat(namespaceStore.get(keyFor(source2Path))).isEqualTo(source2Document);
  }

  @Test
  public void testDeleteSpace() {
    final String spaceName = "space1";
    final List<String> spacePath = List.of(spaceName);
    final Document<String, NameSpaceContainer> spaceDocument = createSpace(spaceName);

    assertThatThrownBy(
            () ->
                DeleteSource.deleteSource(
                    DeleteSource.Options.parse(new String[] {"--name", spaceName}),
                    kvStoreProvider))
        .isInstanceOfSatisfying(
            IllegalStateException.class, e -> assertThat(e.getMessage()).contains("SPACE instead"));

    assertThat(namespaceStore.get(keyFor(spacePath))).isEqualTo(spaceDocument);
  }

  @Test
  public void testDeleteNonRoot() {
    final String sourceName = "source1";
    final List<String> sourcePath = List.of(sourceName);
    final Document<String, NameSpaceContainer> sourceDocument = createSource(sourceName);

    final String datasetName = "dataset1";
    final List<String> datasetPath = List.of(sourceName, datasetName);
    final Document<String, NameSpaceContainer> datasetDocument = createDataset(datasetPath);

    assertThatThrownBy(
            () ->
                DeleteSource.deleteSource(
                    DeleteSource.Options.parse(
                        new String[] {"--name", String.format("%s.%s", sourceName, datasetName)}),
                    kvStoreProvider))
        .isInstanceOfSatisfying(
            IllegalStateException.class, e -> assertThat(e.getMessage()).contains("not found"));

    assertThat(namespaceStore.get(keyFor(sourcePath))).isEqualTo(sourceDocument);
    assertThat(namespaceStore.get(keyFor(datasetPath))).isEqualTo(datasetDocument);
  }

  @Test
  public void testDeleteSourceAlsoDeletesRefreshTimes() throws Exception {
    final String source1Name = "source1";
    final List<String> source1Path = List.of(source1Name);
    createSource(source1Name);
    final String source2Name = "source2";
    final List<String> source2Path = List.of(source2Name);
    Document<String, NameSpaceContainer> source2Document = createSource(source2Name);

    DeleteSource.deleteSource(
        DeleteSource.Options.parse(new String[] {"--name", String.format("%s", source1Name)}),
        kvStoreProvider);

    assertThat(sourceDataStore.get(new NamespaceKey(source1Path))).isNull();
    assertThat(sourceDataStore.get(new NamespaceKey(source2Path))).isNotNull();
  }

  private static String keyFor(List<String> entityPath) {
    return NamespaceServiceImpl.getKey(new NamespaceKey(entityPath));
  }

  private Document<String, NameSpaceContainer> createSource(String sourceName) {
    sourceDataStore.put(new NamespaceKey(sourceName), new SourceInternalData());

    return namespaceStore.put(
        keyFor(List.of(sourceName)),
        new NameSpaceContainer()
            .setFullPathList(List.of(sourceName))
            .setType(Type.SOURCE)
            .setSource(
                new SourceConfig().setId(new EntityId().setId(UUID.randomUUID().toString()))));
  }

  @SuppressWarnings("SameParameterValue") // Might be used later, keep for consistency
  private Document<String, NameSpaceContainer> createSpace(String spaceName) {
    return namespaceStore.put(
        keyFor(List.of(spaceName)),
        new NameSpaceContainer()
            .setFullPathList(List.of(spaceName))
            .setType(Type.SPACE)
            .setSpace(new SpaceConfig().setId(new EntityId().setId(UUID.randomUUID().toString()))));
  }

  private Document<String, NameSpaceContainer> createDataset(List<String> datasetPath) {
    return namespaceStore.put(
        keyFor(datasetPath),
        new NameSpaceContainer()
            .setFullPathList(datasetPath)
            .setType(Type.DATASET)
            .setDataset(
                new DatasetConfig()
                    .setId(new EntityId().setId(UUID.randomUUID().toString()))
                    .setType(DatasetType.PHYSICAL_DATASET)));
  }

  @SuppressWarnings("UnusedReturnValue") // Might be used later, keep for consistency
  private Document<String, NameSpaceContainer> createFolder(List<String> folderPath) {
    return namespaceStore.put(
        keyFor(folderPath),
        new NameSpaceContainer()
            .setFullPathList(folderPath)
            .setType(Type.FOLDER)
            .setFolder(
                new FolderConfig().setId(new EntityId().setId(UUID.randomUUID().toString()))));
  }
}
