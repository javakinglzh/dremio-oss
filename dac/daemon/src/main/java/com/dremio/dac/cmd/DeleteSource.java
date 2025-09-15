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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.dremio.dac.server.DACConfig;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.exec.catalog.CatalogSourceDataCreator;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.catalogpubsub.CatalogEventMessagePublisherProvider;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEventsImpl;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.SourceInternalData;
import com.google.common.annotations.VisibleForTesting;
import java.util.Optional;

/** CLI tool to delete a source while the server is offline. */
@AdminCommand(
    value = "delete-source",
    description =
        "Remove a source plugin from the system. In most cases, sources can and should be removed from a live system instead.",
    hideHelp = true)
public final class DeleteSource {

  @Parameters(separators = "=")
  static final class Options {
    @Parameter(
        names = {"-h", "--help"},
        description = "show usage",
        help = true)
    private boolean help = false;

    @Parameter(
        names = {"-n", "--name"},
        description = "The name of the source to delete",
        required = true)
    private String sourceName = null;

    public static Options parse(String[] cliArgs) {
      final Options options = new Options();
      JCommander jc = JCommander.newBuilder().addObject(options).build();
      jc.setProgramName("dremio-admin delete-source");

      try {
        jc.parse(cliArgs);
      } catch (ParameterException p) {
        AdminLogger.log(p.getMessage());
        jc.usage();
        System.exit(1);
      }

      if (options.help) {
        jc.usage();
        System.exit(0);
      }

      return options;
    }
  }

  private DeleteSource() {}

  private static void go(Options options) throws Exception {
    final DACConfig dacConfig = DACConfig.newConfig();
    if (!dacConfig.isMaster) {
      throw new UnsupportedOperationException("Delete source should be run on master node.");
    }

    try (LocalKVStoreProvider kvStoreProvider = getKvStoreProvider(dacConfig)) {
      kvStoreProvider.start();
      deleteSource(options, kvStoreProvider);
    }
  }

  @VisibleForTesting
  static void deleteSource(Options options, LocalKVStoreProvider kvStoreProvider) throws Exception {
    NamespaceKey sourceKey = new NamespaceKey(options.sourceName);
    NamespaceService systemNamespaceService =
        new NamespaceServiceImpl.Factory(
                kvStoreProvider,
                new CatalogStatusEventsImpl(),
                CatalogEventMessagePublisherProvider.NO_OP)
            .getForSystemUser();

    AdminLogger.log("Finding source [{}]...", options.sourceName);
    NameSpaceContainer entityAtPath = systemNamespaceService.getEntityByPath(sourceKey);
    if (entityAtPath == null) {
      throw new IllegalStateException(String.format("Object at key [%s] not found.", sourceKey));
    }
    if (entityAtPath.getType() != NameSpaceContainer.Type.SOURCE) {
      throw new IllegalStateException(
          String.format(
              "Object at key [%s] is not a source, it is a %s instead.",
              sourceKey, entityAtPath.getType()));
    }
    SourceConfig sourceConfig = entityAtPath.getSource();

    AdminLogger.log("Deleting source [{}]...", options.sourceName);
    systemNamespaceService.deleteSource(sourceKey, sourceConfig.getTag());

    AdminLogger.log("Deleting source [{}] last refresh times...", options.sourceName);
    LegacyKVStore<NamespaceKey, SourceInternalData> sourceDataStore =
        kvStoreProvider.asLegacy().getStore(CatalogSourceDataCreator.class);
    sourceDataStore.delete(sourceKey);

    AdminLogger.log("Deleted source [{}] successfully.", options.sourceName);
  }

  private static LocalKVStoreProvider getKvStoreProvider(DACConfig dacConfig) {
    final Optional<LocalKVStoreProvider> maybeKvStoreProvider =
        CmdUtils.getKVStoreProvider(dacConfig.getConfig());
    if (maybeKvStoreProvider.isEmpty()) {
      throw new IllegalStateException("Failed to delete source. No KVStore detected.");
    }
    return maybeKvStoreProvider.get();
  }

  public static void main(String[] args) {
    try {
      Options parameters = Options.parse(args);
      go(parameters);
      System.exit(0);
    } catch (Exception e) {
      AdminLogger.log("Failed to delete source.", e);
      System.exit(1);
    }
  }
}
