/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.VersionLog;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.RemoveFile;
import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.actions.BaseMigrateDeltaLakeTableActionResult;
import org.apache.iceberg.actions.MigrateDeltaLakeTable;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.TableMigrationUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseMigrateDeltaLakeTableAction implements MigrateDeltaLakeTable {

  private static final Logger LOG = LoggerFactory.getLogger(BaseMigrateDeltaLakeTableAction.class);
  private final String parquetSuffix = ".parquet";
  private final String avroSuffix = ".avro";
  private final String orcSuffix = ".orc";
  private final Map<String, String> additionalProperties = Maps.newHashMap();
  private final DeltaLog deltaLog;
  private final Catalog icebergCatalog;
  private final String deltaTableLocation;
  private final TableIdentifier newTableIdentifier;
  private final Configuration hadoopConfiguration;

  public BaseMigrateDeltaLakeTableAction(
      Catalog icebergCatalog,
      String deltaTableLocation,
      TableIdentifier newTableIdentifier,
      Configuration hadoopConfiguration) {
    this.icebergCatalog = icebergCatalog;
    this.deltaTableLocation = deltaTableLocation;
    this.newTableIdentifier = newTableIdentifier;
    this.hadoopConfiguration = hadoopConfiguration;
    // TODO: check whether we can retrieve hadoopConfiguration directly
    this.deltaLog = DeltaLog.forTable(this.hadoopConfiguration, this.deltaTableLocation);
  }

  @Override
  public MigrateDeltaLakeTable tableProperties(Map<String, String> properties) {
    additionalProperties.putAll(properties);
    return this;
  }

  @Override
  public Result execute() {
    io.delta.standalone.Snapshot updatedSnapshot = deltaLog.update();
    Schema schema = convertDeltaLakeSchema(updatedSnapshot.getMetadata().getSchema());
    PartitionSpec partitionSpec = getPartitionSpecFromDeltaSnapshot(schema);
    // TODO: check whether we need more info when initializing the table
    Table icebergTable =
        this.icebergCatalog.createTable(
            newTableIdentifier,
            schema,
            partitionSpec,
            destTableProperties(
                updatedSnapshot, this.deltaTableLocation, this.additionalProperties));

    copyFromDeltaLakeToIceberg(icebergTable, partitionSpec);

    Snapshot snapshot = icebergTable.currentSnapshot();
    long totalDataFiles =
        Long.parseLong(snapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
    LOG.info(
        "Successfully loaded Iceberg metadata for {} files to {}",
        totalDataFiles,
        deltaTableLocation);
    return new BaseMigrateDeltaLakeTableActionResult(totalDataFiles);
  }

  /** TODO: check the correctness for nested schema */
  private Schema convertDeltaLakeSchema(io.delta.standalone.types.StructType deltaSchema) {
    Type converted =
        DeltaLakeDataTypeVisitor.visit(deltaSchema, new DeltaLakeTypeToType(deltaSchema));
    return new Schema(converted.asNestedType().asStructType().fields());
  }

  private PartitionSpec getPartitionSpecFromDeltaSnapshot(Schema schema) {
    List<String> partitionNames = deltaLog.snapshot().getMetadata().getPartitionColumns();
    if (partitionNames.isEmpty()) {
      return PartitionSpec.unpartitioned();
    }

    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    for (String partitionName : partitionNames) {
      builder.identity(partitionName);
    }
    return builder.build();
  }

  private void copyFromDeltaLakeToIceberg(Table table, PartitionSpec spec) {
    Iterator<VersionLog> it =
        deltaLog.getChanges(
            0, // retrieve actions starting from the initial version
            false); // not throw exception when data loss detected

    while (it.hasNext()) {
      VersionLog versionLog = it.next();
      List<Action> actions = versionLog.getActions();

      // We first need to iterate through to see what kind of transaction this was. There are 3
      // cases:
      // 1. AppendFile - when there are only AddFile instances (an INSERT on the table)
      // 2. DeleteFiles - when there are only RemoveFile instances (a DELETE where all the records
      // of file(s) were removed
      // 3. OverwriteFiles - when there are a mix of AddFile and RemoveFile (a DELETE/UPDATE)

      // Create a map of Delta Lake Action (AddFile, RemoveFile, etc.) --> List<Action>
      Map<String, List<Action>> deltaLakeActionMap =
          actions.stream()
              .filter(action -> action instanceof AddFile || action instanceof RemoveFile)
              .collect(Collectors.groupingBy(a -> a.getClass().getSimpleName()));

      // Scan the map so that we know what type of transaction this will be in Iceberg
      IcebergTransactionType icebergTransactionType =
          getIcebergTransactionTypeFromDeltaActions(deltaLakeActionMap);
      if (icebergTransactionType == null) {
        // TODO: my understanding here is that if the transaction type is undefined,
        //  we can no longer continue even the next versionLog contains valid transaction type
        //  may need further check
        return;
      }

      List<DataFile> filesToAdd = Lists.newArrayList();
      List<DataFile> filesToRemove = Lists.newArrayList();
      for (Action action : Iterables.concat(deltaLakeActionMap.values())) {
        DataFile dataFile = buildDataFileFromAction(action, table, spec);
        if (action instanceof AddFile) {
          filesToAdd.add(dataFile);
        } else if (action instanceof RemoveFile) {
          filesToRemove.add(dataFile);
        } else {
          // TODO: refactor this exception
          throw new RuntimeException("Wrong action type");
        }
      }

      switch (icebergTransactionType) {
        case APPEND_FILES:
          AppendFiles appendFiles = table.newAppend();
          filesToAdd.forEach(appendFiles::appendFile);
          appendFiles.commit();
          break;
        case DELETE_FILES:
          DeleteFiles deleteFiles = table.newDelete();
          filesToRemove.forEach(deleteFiles::deleteFile);
          deleteFiles.commit();
          break;
        case OVERWRITE_FILES:
          OverwriteFiles overwriteFiles = table.newOverwrite();
          filesToAdd.forEach(overwriteFiles::addFile);
          filesToRemove.forEach(overwriteFiles::deleteFile);
          overwriteFiles.commit();
          break;
      }
    }
  }

  @Nullable
  private IcebergTransactionType getIcebergTransactionTypeFromDeltaActions(
      Map<String, List<Action>> actionsMap) {
    IcebergTransactionType icebergTransactionType;
    if (actionsMap.containsKey(AddFile.class.getSimpleName())
        && !actionsMap.containsKey(RemoveFile.class.getSimpleName())) {
      icebergTransactionType = IcebergTransactionType.APPEND_FILES;
    } else if (actionsMap.containsKey(RemoveFile.class.getSimpleName())
        && !actionsMap.containsKey(AddFile.class.getSimpleName())) {
      icebergTransactionType = IcebergTransactionType.DELETE_FILES;
    } else if (actionsMap.containsKey(AddFile.class.getSimpleName())
        && actionsMap.containsKey(RemoveFile.class.getSimpleName())) {
      icebergTransactionType = IcebergTransactionType.OVERWRITE_FILES;
    } else {
      // Some other type of transaction, we can ignore
      return null;
    }
    return icebergTransactionType;
  }

  private DataFile buildDataFileFromAction(Action action, Table table, PartitionSpec spec) {
    String path;
    long size;
    Map<String, String> partitionValues;

    if (action instanceof AddFile) {
      AddFile addFile = (AddFile) action;
      path = addFile.getPath();
      size = addFile.getSize();
      partitionValues = addFile.getPartitionValues();
    } else if (action instanceof RemoveFile) {
      RemoveFile removeFile = (RemoveFile) action;
      path = removeFile.getPath();
      size =
          removeFile
              .getSize()
              .orElseThrow(
                  () ->
                      new RuntimeException(
                          String.format("File %s removed with specifying a size", path)));
      partitionValues =
          Optional.ofNullable(removeFile.getPartitionValues())
              .orElseThrow(
                  () ->
                      new RuntimeException(
                          String.format(
                              "File %s removed without specifying partition values", path)));
    } else {
      throw new IllegalStateException(
          String.format(
              "Unexpected action type for Delta Lake: %s", action.getClass().getSimpleName()));
    }

    String fullFilePath = deltaLog.getPath().toString() + File.separator + path;
    FileFormat format = determineFileFormatFromPath(fullFilePath);

    Metrics metrics = getMetricsForFile(table, fullFilePath, format);
    String partition =
        spec.fields().stream()
            .map(PartitionField::name)
            .map(name -> String.format("%s=%s", name, partitionValues.get(name)))
            .collect(Collectors.joining("/"));

    return DataFiles.builder(spec)
        .withPath(fullFilePath)
        .withFormat(format)
        .withFileSizeInBytes(size)
        .withMetrics(metrics)
        .withPartitionPath(partition)
        .withRecordCount(metrics.recordCount())
        .build();
  }

  protected Metrics getMetricsForFile(Table table, String fullFilePath, FileFormat format) {
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    String nameMappingString = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping nameMapping =
        nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;

    switch (format) {
      case PARQUET:
        return TableMigrationUtil.getParquetMetrics(
            new Path(fullFilePath), this.hadoopConfiguration, metricsConfig, nameMapping);
      case AVRO:
        return TableMigrationUtil.getAvroMetrics(new Path(fullFilePath), this.hadoopConfiguration);
      case ORC:
        return TableMigrationUtil.getOrcMetrics(
            new Path(fullFilePath), this.hadoopConfiguration, metricsConfig, nameMapping);
      default:
        throw new RuntimeException("Unsupported file format: " + format);
    }
  }

  private FileFormat determineFileFormatFromPath(String path) {
    if (path.endsWith(parquetSuffix)) {
      return FileFormat.PARQUET;
    } else if (path.endsWith(avroSuffix)) {
      return FileFormat.AVRO;
    } else if (path.endsWith(orcSuffix)) {
      return FileFormat.ORC;
    } else {
      throw new RuntimeException("The format of the file is unsupported: " + path);
    }
  }

  private static Map<String, String> destTableProperties(
      io.delta.standalone.Snapshot deltaSnapshot,
      String tableLocation,
      Map<String, String> additionalProperties) {
    Map<String, String> properties = Maps.newHashMap();

    properties.putAll(deltaSnapshot.getMetadata().getConfiguration());
    properties.putAll(
        ImmutableMap.of(
            "migration_source", "delta", "table_type", "iceberg", "location", tableLocation));
    properties.putAll(additionalProperties);

    return properties;
  }

  private enum IcebergTransactionType {
    APPEND_FILES,
    DELETE_FILES,
    OVERWRITE_FILES
  }
}
