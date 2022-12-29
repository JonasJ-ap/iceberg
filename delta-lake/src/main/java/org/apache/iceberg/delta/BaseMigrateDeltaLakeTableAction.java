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
package org.apache.iceberg.delta;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.VersionLog;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.RemoveFile;
import java.io.File;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.delta.utils.DeltaLakeDataTypeVisitor;
import org.apache.iceberg.delta.utils.DeltaLakeTypeToType;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.orc.OrcMetrics;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes a Delta Lake table's location and attempts to transform it into an Iceberg table in an
 * optional user-specified location (default to the Delta Lake table's location) with a different
 * identifier.
 */
public class BaseMigrateDeltaLakeTableAction implements MigrateDeltaLakeTable {

  private static final Logger LOG = LoggerFactory.getLogger(BaseMigrateDeltaLakeTableAction.class);

  private static final String MIGRATION_SOURCE_PROP = "migration_source";
  private static final String DELTA_SOURCE_VALUE = "delta";
  private static final String ORIGINAL_LOCATION_PROP = "original_location";
  private static final String PARQUET_SUFFIX = ".parquet";
  private static final String AVRO_SUFFIX = ".avro";
  private static final String ORC_SUFFIX = ".orc";
  private final Map<String, String> additionalProperties = Maps.newHashMap();
  private final DeltaLog deltaLog;
  private final Catalog icebergCatalog;
  private final String deltaTableLocation;
  private final TableIdentifier newTableIdentifier;
  private final Configuration hadoopConfiguration;
  private final String newTableLocation;

  public BaseMigrateDeltaLakeTableAction(
      Catalog icebergCatalog,
      String deltaTableLocation,
      TableIdentifier newTableIdentifier,
      Configuration hadoopConfiguration) {
    this.icebergCatalog = icebergCatalog;
    this.deltaTableLocation = deltaTableLocation;
    this.newTableIdentifier = newTableIdentifier;
    this.hadoopConfiguration = hadoopConfiguration;
    this.newTableLocation = deltaTableLocation;
    this.deltaLog = DeltaLog.forTable(this.hadoopConfiguration, this.deltaTableLocation);
  }

  public BaseMigrateDeltaLakeTableAction(
      Catalog icebergCatalog,
      String deltaTableLocation,
      TableIdentifier newTableIdentifier,
      String newTableLocation,
      Configuration hadoopConfiguration) {
    this.icebergCatalog = icebergCatalog;
    this.deltaTableLocation = deltaTableLocation;
    this.newTableIdentifier = newTableIdentifier;
    this.hadoopConfiguration = hadoopConfiguration;
    this.newTableLocation = newTableLocation;
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
    Table icebergTable =
        this.icebergCatalog.createTable(
            newTableIdentifier,
            schema,
            partitionSpec,
            this.newTableLocation,
            destTableProperties(
                updatedSnapshot, this.deltaTableLocation, this.additionalProperties));

    copyFromDeltaLakeToIceberg(icebergTable, partitionSpec);

    Snapshot snapshot = icebergTable.currentSnapshot();
    long totalDataFiles =
        Long.parseLong(snapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
    LOG.info(
        "Successfully loaded Iceberg metadata for {} files in {}",
        totalDataFiles,
        deltaTableLocation);
    return new BaseMigrateDeltaLakeTableActionResult(totalDataFiles);
  }

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
    // Make the migration process into one transaction
    Transaction transaction = table.newTransaction();
    Iterator<VersionLog> versionLogIterator =
        deltaLog.getChanges(
            0, // retrieve actions starting from the initial version
            false); // not throw exception when data loss detected

    while (versionLogIterator.hasNext()) {
      VersionLog versionLog = versionLogIterator.next();
      commitDeltaVersionLogToIcebergTransaction(versionLog, transaction, table, spec);
    }

    // commit transaction once all dataFiles are registered.
    transaction.commitTransaction();
  }

  private void commitDeltaVersionLogToIcebergTransaction(
      VersionLog versionLog, Transaction transaction, Table table, PartitionSpec spec) {
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

    List<DataFile> filesToAdd = Lists.newArrayList();
    List<DataFile> filesToRemove = Lists.newArrayList();
    for (Action action : Iterables.concat(deltaLakeActionMap.values())) {
      DataFile dataFile = buildDataFileFromAction(action, table, spec);
      if (action instanceof AddFile) {
        filesToAdd.add(dataFile);
      } else if (action instanceof RemoveFile) {
        filesToRemove.add(dataFile);
      } else {
        throw new ValidationException(
            "The action %s's is unsupported", action.getClass().getSimpleName());
      }
    }

    if (filesToAdd.size() > 0 && filesToRemove.size() > 0) {
      // Overwrite_Files case
      OverwriteFiles overwriteFiles = transaction.newOverwrite();
      filesToAdd.forEach(overwriteFiles::addFile);
      filesToRemove.forEach(overwriteFiles::deleteFile);
      overwriteFiles.commit();
    } else if (filesToAdd.size() > 0) {
      // Append_Files case
      AppendFiles appendFiles = transaction.newAppend();
      filesToAdd.forEach(appendFiles::appendFile);
      appendFiles.commit();
    } else if (filesToRemove.size() > 0) {
      // Delete_Files case
      DeleteFiles deleteFiles = transaction.newDelete();
      filesToRemove.forEach(deleteFiles::deleteFile);
      deleteFiles.commit();
    }
  }

  public DataFile buildDataFileFromAction(Action action, Table table, PartitionSpec spec) {
    String path;
    Map<String, String> partitionValues;

    if (action instanceof AddFile) {
      AddFile addFile = (AddFile) action;
      path = addFile.getPath();
      partitionValues = addFile.getPartitionValues();
    } else if (action instanceof RemoveFile) {
      RemoveFile removeFile = (RemoveFile) action;
      path = removeFile.getPath();
      partitionValues = removeFile.getPartitionValues();
    } else {
      throw new IllegalStateException(
          String.format(
              "Unexpected action type for Delta Lake: %s", action.getClass().getSimpleName()));
    }

    if (partitionValues == null) {
      // For unpartitioned table, the partitionValues should be an empty map rather than null
      throw new ValidationException("File %s does not specify a partitionValues", path);
    }

    String fullFilePath = deltaLog.getPath().toString() + File.separator + path;
    FileFormat format = determineFileFormatFromPath(fullFilePath);
    FileIO io = table.io();
    InputFile file;
    if (io != null) {
      file = io.newInputFile(fullFilePath);
    } else {
      file = HadoopInputFile.fromPath(new Path(fullFilePath), this.hadoopConfiguration);
    }

    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    String nameMappingString = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping nameMapping =
        nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;
    Metrics metrics = getMetricsForFile(file, format, metricsConfig, nameMapping);

    String partition =
        spec.fields().stream()
            .map(PartitionField::name)
            .map(name -> String.format("%s=%s", name, partitionValues.get(name)))
            .collect(Collectors.joining("/"));

    return DataFiles.builder(spec)
        .withPath(fullFilePath)
        .withFormat(format)
        .withFileSizeInBytes(file.getLength())
        .withMetrics(metrics)
        .withPartitionPath(partition)
        .build();
  }

  private FileFormat determineFileFormatFromPath(String path) {
    if (path.endsWith(PARQUET_SUFFIX)) {
      return FileFormat.PARQUET;
    } else if (path.endsWith(AVRO_SUFFIX)) {
      return FileFormat.AVRO;
    } else if (path.endsWith(ORC_SUFFIX)) {
      return FileFormat.ORC;
    } else {
      throw new ValidationException("The format of the file %s is unsupported", path);
    }
  }

  private Metrics getMetricsForFile(
      InputFile file, FileFormat format, MetricsConfig metricsSpec, NameMapping mapping) {
    try {
      switch (format) {
        case AVRO:
          long rowCount = Avro.rowCount(file);
          return new Metrics(rowCount, null, null, null, null);
        case PARQUET:
          return ParquetUtil.fileMetrics(file, metricsSpec, mapping);
        case ORC:
          return OrcMetrics.fromInputFile(file, metricsSpec, mapping);
        default:
          throw new ValidationException("Unsupported file format: %s", format);
      }
    } catch (UncheckedIOException e) {
      throw new RuntimeException(
          String.format(
              "Unable to read the metrics of the %s file: %s", format.name(), file.location()),
          e);
    }
  }

  private static Map<String, String> destTableProperties(
      io.delta.standalone.Snapshot deltaSnapshot,
      String originalLocation,
      Map<String, String> additionalProperties) {
    Map<String, String> properties = Maps.newHashMap();

    properties.putAll(deltaSnapshot.getMetadata().getConfiguration());
    properties.putAll(
        ImmutableMap.of(
            MIGRATION_SOURCE_PROP, DELTA_SOURCE_VALUE, ORIGINAL_LOCATION_PROP, originalLocation));
    properties.putAll(additionalProperties);

    return properties;
  }
}
