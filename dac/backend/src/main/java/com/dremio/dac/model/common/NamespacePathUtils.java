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
package com.dremio.dac.model.common;

import static com.dremio.common.utils.PathUtils.encodeURIComponent;
import static com.dremio.service.namespace.NamespaceUtils.isHomeSpace;

import com.dremio.catalog.model.VersionedDatasetId;
import com.dremio.dac.explore.model.DatasetName;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.folder.FolderName;
import com.dremio.dac.model.folder.FolderPath;
import com.dremio.dac.model.folder.SourceFolderPath;
import com.dremio.dac.model.job.JobFilters;
import com.dremio.dac.model.sources.PhysicalDatasetPath;
import com.dremio.dac.model.sources.SourceName;
import com.dremio.dac.model.sources.VirtualDatasetPath;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.model.spaces.SpaceName;
import com.dremio.dac.model.spaces.TempSpace;
import com.dremio.file.FilePath;
import com.dremio.file.SourceFilePath;
import com.dremio.service.jobs.JobIndexKeys;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.jetbrains.annotations.NotNull;

/** Utility methods for namespace paths. */
public final class NamespacePathUtils {

  /**
   * Constructs a {@link NamespacePath} for a dataset based on its container, type and path
   * components.
   *
   * @param datasetType the type of the dataset (virtual, physical, file, etc.)
   * @param fullPath the full path of the dataset as a list of string components
   * @param rootContainerType the container type (e.g., SOURCE, SPACE, HOME) of the root entity
   * @return a concrete {@link NamespacePath} representing the dataset
   * @throws RuntimeException if the dataset type is invalid
   */
  public static NamespacePath getNamespacePathForDataType(
      DatasetType datasetType, List<String> fullPath, NameSpaceContainer.Type rootContainerType) {
    List<String> components = new ArrayList<>(fullPath);
    String leafName = components.remove(fullPath.size() - 1);
    String rootName = components.remove(0);
    @NotNull
    List<FolderName> folderPath =
        components.stream().map(FolderName::new).collect(Collectors.toList());
    switch (datasetType) {
      case VIRTUAL_DATASET:
        RootEntity rootEntity = getRootEntity(rootName, rootContainerType);
        return new DatasetPath(rootEntity, folderPath, new DatasetName(leafName));
      case PHYSICAL_DATASET:
        return new PhysicalDatasetPath(fullPath);
      case PHYSICAL_DATASET_SOURCE_FILE:
        return new SourceFilePath(fullPath);
      case PHYSICAL_DATASET_SOURCE_FOLDER:
        return new SourceFolderPath(fullPath);
      case PHYSICAL_DATASET_HOME_FILE:
        return new FolderPath(fullPath);
      case PHYSICAL_DATASET_HOME_FOLDER:
        return new FilePath(fullPath);
      default:
        throw new RuntimeException("Invalid dataset type");
    }
  }

  /**
   * Resolves the appropriate {@link RootEntity} (e.g., Space, Home, Temp, Source) from a root path
   * name and its namespace container type.
   *
   * @param name the root name of the entity
   * @param rootContainerType the type of namespace container
   * @return the resolved {@link RootEntity}
   * @throws IllegalArgumentException if the container type does not match a known root entity
   */
  public static RootEntity getRootEntity(String name, NameSpaceContainer.Type rootContainerType) {
    if (TempSpace.isTempSpace(name)) {
      return TempSpace.impl();
    } else if (isHomeSpace(name)) {
      return new HomeName(name);
    } else if (rootContainerType == NameSpaceContainer.Type.SOURCE) {
      return new SourceName(name);
    } else if (rootContainerType == NameSpaceContainer.Type.SPACE) {
      return new SpaceName(name);
    } else {
      throw new IllegalArgumentException(
          "Unexpected rootContainerType: " + rootContainerType + ", for name: " + name);
    }
  }

  /**
   * Creates a map of REST-style relative links related to a dataset, including a query link.
   * Primarily used by the UI to display navigation options or api resources.
   *
   * <p>Equivalent to {@link #createLinks} but always includes a `"query"` link.
   *
   * @param fullPath the complete dataset path
   * @param datasetVersion the version of the dataset
   * @param datasetType the type of the dataset
   * @param rootContainerType the container type of the dataset's root
   * @return a map of relative URI paths (e.g., "self", "edit", "jobs", "query")
   */
  public static Map<String, String> createLinksWithQueryLink(
      List<String> fullPath,
      @Nullable DatasetVersion datasetVersion,
      DatasetType datasetType,
      NameSpaceContainer.Type rootContainerType) {
    return createLinks(
        fullPath, fullPath, datasetVersion, true, false, null, datasetType, rootContainerType);
  }

  /**
   * Creates a map of REST-style relative links related to a dataset for use by the UI. This may
   * include links to self, jobs, query editor, and edit mode depending on dataset type and context.
   *
   * @param fullPath the actual physical or logical path to the dataset
   * @param displayFullPath the display version of the full path (may differ for virtual datasets)
   * @param datasetVersion the dataset version (nullable)
   * @param includeQueryLink whether to include a `"query"` link in the output
   * @param isUnsavedDirectPhysicalDataset whether the dataset is a newly created, unsaved physical
   *     dataset
   * @param entityId the optional entity ID used to detect if dataset is versioned
   * @param datasetType the dataset type (e.g., VIRTUAL_DATASET, PHYSICAL_DATASET)
   * @param rootContainerType the root container type (e.g., SOURCE, SPACE)
   * @return a map of relative URI links (e.g., "self", "query", "jobs", "edit")
   */
  public static Map<String, String> createLinks(
      List<String> fullPath,
      List<String> displayFullPath,
      @Nullable DatasetVersion datasetVersion,
      boolean includeQueryLink,
      boolean isUnsavedDirectPhysicalDataset,
      String entityId,
      DatasetType datasetType,
      NameSpaceContainer.Type rootContainerType) {
    String namespacePath;

    final boolean isVersionedDataset = VersionedDatasetId.tryParse(entityId) != null;
    if (isUnsavedDirectPhysicalDataset) {
      if (isHomeSpace(displayFullPath.get(0))) {
        namespacePath = new DatasetPath(displayFullPath).getQueryUrlPath();
      } else {
        namespacePath = new PhysicalDatasetPath(displayFullPath).getQueryUrlPath();
      }
    } else if (isVersionedDataset) {
      namespacePath =
          ((datasetType == DatasetType.VIRTUAL_DATASET)
              ? new VirtualDatasetPath(displayFullPath).getQueryUrlPath()
              : new PhysicalDatasetPath(displayFullPath).getQueryUrlPath());
    } else {
      namespacePath =
          getNamespacePathForDataType(datasetType, displayFullPath, rootContainerType)
              .getQueryUrlPath();
    }

    Map<String, String> links = new HashMap<>();
    String versionValue =
        (datasetVersion != null) ? encodeURIComponent(datasetVersion.toString()) : null;

    links.put("self", namespacePath + "?version=" + versionValue);
    links.put("jobs", getJobsUrl(fullPath));
    if (includeQueryLink) {
      links.put("query", namespacePath);
    }
    // Add "edit" link only for virtual datasets
    if (datasetType == DatasetType.VIRTUAL_DATASET) {
      links.put("edit", namespacePath + "?mode=edit&version=" + versionValue);
    }

    return links;
  }

  /**
   * Constructs a URL pointing to the jobs history filtered for the given dataset path.
   *
   * @param fullPath the dataset path
   * @return a relative URI to the filtered jobs list
   */
  private static String getJobsUrl(List<String> fullPath) {
    final NamespaceKey datasetPath = new NamespaceKey(fullPath);
    final JobFilters jobFilters =
        new JobFilters()
            .addFilter(JobIndexKeys.ALL_DATASETS, datasetPath.toString())
            .addFilter(JobIndexKeys.QUERY_TYPE, JobIndexKeys.UI, JobIndexKeys.EXTERNAL);
    return jobFilters.toUrl();
  }
}
