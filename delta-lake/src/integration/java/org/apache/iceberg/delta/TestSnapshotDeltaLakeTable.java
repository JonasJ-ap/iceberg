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

import static org.apache.spark.sql.functions.current_date;
import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.expr;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.RemoveFile;
import io.delta.standalone.exceptions.DeltaConcurrentModificationException;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.delta.catalog.DeltaCatalog;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSnapshotDeltaLakeTable extends SparkDeltaLakeSnapshotTestBase {
  private static final String SNAPSHOT_SOURCE_PROP = "snapshot_source";
  private static final String DELTA_SOURCE_VALUE = "delta";
  private static final String ORIGINAL_LOCATION_PROP = "original_location";
  private static final String NAMESPACE = "delta_conversion_test";
  private static final String defaultSparkCatalog = "spark_catalog";
  private static final String icebergCatalogName = "iceberg_hive";
  private String partitionedIdentifier;
  private String unpartitionedIdentifier;
  private String externalDataFilesIdentifier;
  private String typeTestIdentifier;
  private final String partitionedTableName = "partitioned_table";
  private final String unpartitionedTableName = "unpartitioned_table";
  private final String externalDataFilesTableName = "external_data_files_table";
  private final String typeTestTableName = "type_test_table";
  private final String snapshotPartitionedTableName = "iceberg_partitioned_table";
  private final String snapshotUnpartitionedTableName = "iceberg_unpartitioned_table";
  private final String snapshotExternalDataFilesTableName = "iceberg_external_data_files_table";
  private final String snapshotNewTableLocationTableName = "iceberg_new_table_location_table";
  private final String snapshotAdditionalPropertiesTableName =
      "iceberg_additional_properties_table";
  private final String snapshotTypeTestTableName = "iceberg_type_test_table";
  private String partitionedLocation;
  private String unpartitionedLocation;
  private String newIcebergTableLocation;
  private String externalDataFilesTableLocation;
  private String typeTestTableLocation;

  @Parameterized.Parameters(name = "Catalog Name {0} - Options {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      new Object[] {
        icebergCatalogName,
        SparkCatalog.class.getName(),
        ImmutableMap.of(
            "type",
            "hive",
            "default-namespace",
            "default",
            "parquet-enabled",
            "true",
            "cache-enabled",
            "false" // Spark will delete tables using v1, leaving the cache out of sync
            )
      }
    };
  }

  @Rule public TemporaryFolder temp1 = new TemporaryFolder();
  @Rule public TemporaryFolder temp2 = new TemporaryFolder();
  @Rule public TemporaryFolder temp3 = new TemporaryFolder();
  @Rule public TemporaryFolder temp4 = new TemporaryFolder();
  @Rule public TemporaryFolder temp5 = new TemporaryFolder();

  public TestSnapshotDeltaLakeTable(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
    spark.conf().set("spark.sql.catalog." + defaultSparkCatalog, DeltaCatalog.class.getName());
  }

  @Before
  public void before() throws IOException {
    File partitionedFolder = temp1.newFolder();
    File unpartitionedFolder = temp2.newFolder();
    File newIcebergTableFolder = temp3.newFolder();
    File externalDataFilesTableFolder = temp4.newFolder();
    File typeTestTableFolder = temp5.newFolder();
    partitionedLocation = partitionedFolder.toURI().toString();
    unpartitionedLocation = unpartitionedFolder.toURI().toString();
    newIcebergTableLocation = newIcebergTableFolder.toURI().toString();
    externalDataFilesTableLocation = externalDataFilesTableFolder.toURI().toString();
    typeTestTableLocation = typeTestTableFolder.toURI().toString();

    spark.sql(String.format("CREATE DATABASE IF NOT EXISTS %s", NAMESPACE));

    partitionedIdentifier = destName(defaultSparkCatalog, partitionedTableName);
    unpartitionedIdentifier = destName(defaultSparkCatalog, unpartitionedTableName);
    externalDataFilesIdentifier = destName(defaultSparkCatalog, externalDataFilesTableName);
    typeTestIdentifier = destName(defaultSparkCatalog, typeTestTableName);

    spark.sql(String.format("DROP TABLE IF EXISTS %s", partitionedIdentifier));
    spark.sql(String.format("DROP TABLE IF EXISTS %s", unpartitionedIdentifier));
    spark.sql(String.format("DROP TABLE IF EXISTS %s", externalDataFilesIdentifier));
    spark.sql(String.format("DROP TABLE IF EXISTS %s", typeTestIdentifier));

    // hard code the dataframe
    Dataset<Row> df = nestedDataframe();

    // write to delta tables
    df.write()
        .format("delta")
        .mode(SaveMode.Append)
        .partitionBy("id")
        .option("path", partitionedLocation)
        .saveAsTable(partitionedIdentifier);

    df.write()
        .format("delta")
        .mode(SaveMode.Append)
        .option("path", unpartitionedLocation)
        .saveAsTable(unpartitionedIdentifier);

    df.write()
        .format("delta")
        .mode(SaveMode.Append)
        .option("path", externalDataFilesTableLocation)
        .saveAsTable(externalDataFilesIdentifier);

    spark
        .range(0, 10, 1, 10)
        .withColumnRenamed("id", "longCol")
        .withColumn("intCol", expr("CAST(longCol AS INT)"))
        .withColumn("floatCol", expr("CAST(longCol AS FLOAT)"))
        .withColumn("doubleCol", expr("CAST(longCol AS DOUBLE)"))
        .withColumn("dateCol", date_add(current_date(), 1))
        .withColumn("timestampCol", expr("TO_TIMESTAMP(dateCol)"))
        .withColumn("stringCol", expr("CAST(dateCol AS STRING)"))
        .withColumn("booleanCol", expr("longCol > 5"))
        .withColumn("binaryCol", expr("CAST(longCol AS BINARY)"))
        .withColumn("byteCol", expr("CAST(longCol AS BYTE)"))
        .withColumn("decimalCol", expr("CAST(longCol AS DECIMAL(10, 2))"))
        .withColumn("shortCol", expr("CAST(longCol AS SHORT)"))
        .withColumn("mapCol", expr("MAP(longCol, decimalCol)"))
        .withColumn("arrayCol", expr("ARRAY(longCol)"))
        .withColumn("structCol", expr("STRUCT(mapCol, arrayCol)"))
        .write()
        .format("delta")
        .mode("append")
        .option("path", typeTestTableLocation)
        .saveAsTable(typeTestIdentifier);

    // Delete a record from the table
    spark.sql("DELETE FROM " + partitionedIdentifier + " WHERE id=3");
    spark.sql("DELETE FROM " + unpartitionedIdentifier + " WHERE id=3");

    // Update a record
    spark.sql("UPDATE " + partitionedIdentifier + " SET id=3 WHERE id=1");
    spark.sql("UPDATE " + unpartitionedIdentifier + " SET id=3 WHERE id=1");
  }

  @After
  public void after() {
    // Drop delta lake tables.
    spark.sql(
        String.format(
            "DROP TABLE IF EXISTS %s", destName(defaultSparkCatalog, partitionedTableName)));
    spark.sql(
        String.format(
            "DROP TABLE IF EXISTS %s", destName(defaultSparkCatalog, unpartitionedTableName)));
    spark.sql(
        String.format(
            "DROP TABLE IF EXISTS %s", destName(defaultSparkCatalog, externalDataFilesTableName)));
    spark.sql(
        String.format("DROP TABLE IF EXISTS %s", destName(defaultSparkCatalog, typeTestTableName)));

    // Drop iceberg tables.
    spark.sql(
        String.format(
            "DROP TABLE IF EXISTS %s", destName(icebergCatalogName, snapshotPartitionedTableName)));
    spark.sql(
        String.format(
            "DROP TABLE IF EXISTS %s",
            destName(icebergCatalogName, snapshotUnpartitionedTableName)));
    spark.sql(
        String.format(
            "DROP TABLE IF EXISTS %s",
            destName(icebergCatalogName, snapshotExternalDataFilesTableName)));
    spark.sql(
        String.format(
            "DROP TABLE IF EXISTS %s",
            destName(icebergCatalogName, snapshotNewTableLocationTableName)));
    spark.sql(
        String.format(
            "DROP TABLE IF EXISTS %s",
            destName(icebergCatalogName, snapshotAdditionalPropertiesTableName)));
    spark.sql(
        String.format(
            "DROP TABLE IF EXISTS %s", destName(icebergCatalogName, snapshotTypeTestTableName)));

    spark.sql(String.format("DROP DATABASE IF EXISTS %s", NAMESPACE));
  }

  @Test
  public void testBasicSnapshotPartitioned() {
    String newTableIdentifier = destName(icebergCatalogName, snapshotPartitionedTableName);
    SnapshotDeltaLakeTable.Result result =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, partitionedLocation)
            .execute();

    checkSnapshotIntegrity(partitionedLocation, partitionedIdentifier, newTableIdentifier, result);
    checkIcebergTableLocation(newTableIdentifier, partitionedLocation);
  }

  @Test
  public void testBasicSnapshotUnpartitioned() {
    String newTableIdentifier = destName(icebergCatalogName, snapshotUnpartitionedTableName);
    SnapshotDeltaLakeTable.Result result =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, unpartitionedLocation)
            .execute();

    checkSnapshotIntegrity(
        unpartitionedLocation, unpartitionedIdentifier, newTableIdentifier, result);
    checkIcebergTableLocation(newTableIdentifier, unpartitionedLocation);
  }

  @Test
  public void testSnapshotWithNewLocation() {
    String newTableIdentifier = destName(icebergCatalogName, snapshotNewTableLocationTableName);
    SnapshotDeltaLakeTable.Result result =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, partitionedLocation)
            .tableLocation(newIcebergTableLocation)
            .execute();

    checkSnapshotIntegrity(partitionedLocation, partitionedIdentifier, newTableIdentifier, result);
    checkIcebergTableLocation(newTableIdentifier, newIcebergTableLocation);
  }

  @Test
  public void testSnapshotWithAdditionalProperties() {
    // add some properties to the original delta table
    spark.sql(
        "ALTER TABLE "
            + unpartitionedIdentifier
            + " SET TBLPROPERTIES ('foo'='bar', 'test0'='test0')");
    String newTableIdentifier = destName(icebergCatalogName, snapshotAdditionalPropertiesTableName);
    SnapshotDeltaLakeTable.Result result =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, unpartitionedLocation)
            .tableProperty("test1", "test1")
            .tableProperties(
                ImmutableMap.of(
                    "test2", "test2", "test3", "test3", "test4",
                    "test4")) // add additional iceberg table properties
            .execute();

    checkSnapshotIntegrity(
        unpartitionedLocation, unpartitionedIdentifier, newTableIdentifier, result);
    checkIcebergTableLocation(newTableIdentifier, unpartitionedLocation);
    checkIcebergTableProperties(
        newTableIdentifier,
        ImmutableMap.of(
            "foo", "bar", "test0", "test0", "test1", "test1", "test2", "test2", "test3", "test3",
            "test4", "test4"),
        unpartitionedLocation);
  }

  @Test
  public void testSnapshotTableWithExternalDataFiles() {
    // Add parquet files to default.external_data_files_table. The newly added parquet files
    // are not at the same location as the table.
    addExternalDatafiles(externalDataFilesTableLocation, unpartitionedLocation);

    String newTableIdentifier = destName(icebergCatalogName, snapshotExternalDataFilesTableName);
    SnapshotDeltaLakeTable.Result result =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, externalDataFilesTableLocation)
            .execute();
    checkSnapshotIntegrity(
        externalDataFilesTableLocation, externalDataFilesIdentifier, newTableIdentifier, result);
    checkIcebergTableLocation(newTableIdentifier, externalDataFilesTableLocation);
    checkDataFilePathsIntegrity(newTableIdentifier, externalDataFilesTableLocation);
  }

  @Test
  public void testSnapshotSupportedTypes() {
    String newTableIdentifier = destName(icebergCatalogName, snapshotTypeTestTableName);
    SnapshotDeltaLakeTable.Result result =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, typeTestTableLocation)
            .tableProperty(TableProperties.PARQUET_VECTORIZATION_ENABLED, "false")
            .execute();
    checkSnapshotIntegrity(typeTestTableLocation, typeTestIdentifier, newTableIdentifier, result);
    checkIcebergTableLocation(newTableIdentifier, typeTestTableLocation);
    checkIcebergTableProperties(
        newTableIdentifier,
        ImmutableMap.of(TableProperties.PARQUET_VECTORIZATION_ENABLED, "false"),
        typeTestTableLocation);
  }

  private void checkSnapshotIntegrity(
      String deltaTableLocation,
      String deltaTableIdentifier,
      String icebergTableIdentifier,
      SnapshotDeltaLakeTable.Result snapshotReport) {
    DeltaLog deltaLog = DeltaLog.forTable(spark.sessionState().newHadoopConf(), deltaTableLocation);

    List<Row> deltaTableContents =
        spark.sql("SELECT * FROM " + deltaTableIdentifier).collectAsList();
    List<Row> icebergTableContents =
        spark.sql("SELECT * FROM " + icebergTableIdentifier).collectAsList();

    Assertions.assertThat(deltaTableContents).hasSize(icebergTableContents.size());
    Assertions.assertThat(deltaLog.update().getAllFiles())
        .hasSize((int) snapshotReport.snapshotDataFilesCount());
    Assertions.assertThat(icebergTableContents).containsAll(deltaTableContents);
    Assertions.assertThat(deltaTableContents).containsAll(icebergTableContents);
  }

  private void checkIcebergTableLocation(String icebergTableIdentifier, String expectedLocation) {
    Table icebergTable = getIcebergTable(icebergTableIdentifier);
    Assertions.assertThat(icebergTable.location()).isEqualTo(expectedLocation);
  }

  private void checkIcebergTableProperties(
      String icebergTableIdentifier,
      Map<String, String> expectedAdditionalProperties,
      String deltaTableLocation) {
    Table icebergTable = getIcebergTable(icebergTableIdentifier);
    ImmutableMap.Builder<String, String> expectedPropertiesBuilder = ImmutableMap.builder();
    // The snapshot action will put some fixed properties to the table
    expectedPropertiesBuilder.put(SNAPSHOT_SOURCE_PROP, DELTA_SOURCE_VALUE);
    expectedPropertiesBuilder.putAll(expectedAdditionalProperties);
    ImmutableMap<String, String> expectedProperties = expectedPropertiesBuilder.build();

    Assertions.assertThat(icebergTable.properties().entrySet())
        .containsAll(expectedProperties.entrySet());
    Assertions.assertThat(icebergTable.properties())
        .containsEntry(ORIGINAL_LOCATION_PROP, deltaTableLocation);
  }

  private void checkDataFilePathsIntegrity(
      String icebergTableIdentifier, String deltaTableLocation) {
    Table icebergTable = getIcebergTable(icebergTableIdentifier);
    DeltaLog deltaLog = DeltaLog.forTable(spark.sessionState().newHadoopConf(), deltaTableLocation);
    // checkSnapshotIntegrity already checks the number of data files in the snapshot iceberg table
    // equals that in the original delta lake table
    List<String> deltaTableDataFilePaths =
        deltaLog.update().getAllFiles().stream()
            .map(f -> getFullFilePath(f.getPath(), deltaLog.getPath().toString()))
            .collect(Collectors.toList());
    icebergTable
        .currentSnapshot()
        .addedDataFiles(icebergTable.io())
        .forEach(
            dataFile -> {
              Assertions.assertThat(deltaTableDataFilePaths).contains(dataFile.path().toString());
            });
  }

  private Table getIcebergTable(String icebergTableIdentifier) {
    CatalogPlugin defaultCatalog = spark.sessionState().catalogManager().currentCatalog();
    Spark3Util.CatalogAndIdentifier catalogAndIdent =
        Spark3Util.catalogAndIdentifier(
            "test catalog", spark, icebergTableIdentifier, defaultCatalog);
    return Spark3Util.loadIcebergCatalog(spark, catalogAndIdent.catalog().name())
        .loadTable(TableIdentifier.parse(catalogAndIdent.identifier().toString()));
  }

  private String destName(String catalogName, String dest) {
    if (catalogName.equals(defaultSparkCatalog)) {
      return NAMESPACE + "." + catalogName + "_" + dest;
    }
    return catalogName + "." + NAMESPACE + "." + catalogName + "_" + dest;
  }

  /**
   * Add parquet files manually to a delta lake table to mock the situation that some data files are
   * not in the same location as the delta lake table. The case that {@link AddFile#getPath()} or
   * {@link RemoveFile#getPath()} returns absolute path.
   *
   * <p>The known <a href="https://github.com/delta-io/connectors/issues/380">issue</a> makes it
   * necessary to manually rebuild the AddFile to avoid deserialization error when committing the
   * transaction.
   */
  private void addExternalDatafiles(
      String targetDeltaTableLocation, String sourceDeltaTableLocation) {
    DeltaLog targetLog =
        DeltaLog.forTable(spark.sessionState().newHadoopConf(), targetDeltaTableLocation);
    OptimisticTransaction transaction = targetLog.startTransaction();
    DeltaLog sourceLog =
        DeltaLog.forTable(spark.sessionState().newHadoopConf(), sourceDeltaTableLocation);
    List<AddFile> newFiles =
        sourceLog.update().getAllFiles().stream()
            .map(
                f ->
                    AddFile.builder(
                            getFullFilePath(f.getPath(), sourceLog.getPath().toString()),
                            f.getPartitionValues(),
                            f.getSize(),
                            System.currentTimeMillis(),
                            true)
                        .build())
            .collect(Collectors.toList());
    try {
      transaction.commit(newFiles, new Operation(Operation.Name.UPDATE), "Delta-Lake/2.2.0");
    } catch (DeltaConcurrentModificationException e) {
      throw new RuntimeException(e);
    }
  }

  private static String getFullFilePath(String path, String tableRoot) {
    URI dataFileUri = URI.create(path);
    if (dataFileUri.isAbsolute()) {
      return path;
    } else {
      return tableRoot + File.separator + path;
    }
  }

  /**
   * Hardcode a nested dataframe to test the snapshot feature. The schema of created dataframe is:
   *
   * <pre>
   *  root
   *  |-- address_nested: struct (nullable = true)
   *  |    |-- current: struct (nullable = true)
   *  |    |    |-- city: string (nullable = true)
   *  |    |    |-- state: string (nullable = true)
   *  |    |-- previous: struct (nullable = true)
   *  |    |    |-- city: string (nullable = true)
   *  |    |    |-- state: string (nullable = true)
   *  |-- addresses: array (nullable = true)
   *  |    |-- element: struct (containsNull = true)
   *  |    |    |-- city: string (nullable = true)
   *  |    |    |-- state: string (nullable = true)
   *  |-- id: long (nullable = true)
   *  |-- magic_number: double (nullable = true)
   *  |-- name: string (nullable = true)
   *  |-- properties: struct (nullable = true)
   *  |    |-- eye: string (nullable = true)
   *  |    |-- hair: string (nullable = true)
   *  |-- secondProp: struct (nullable = true)
   *  |    |-- height: string (nullable = true)
   *  |-- subjects: array (nullable = true)
   *  |    |-- element: array (containsNull = true)
   *  |    |    |-- element: string (containsNull = true)
   * </pre>
   *
   * The dataframe content is (by calling df.show()):
   *
   * <pre>
   * +--------------------+--------------------+---+--------------+-------+--------------------+----------+--------------------+
   * |      address_nested|           addresses| id|  magic_number|   name|          properties|secondProp|            subjects|
   * +--------------------+--------------------+---+--------------+-------+--------------------+----------+--------------------+
   * |{{NewYork, NY}, {...|[{SanJose, CA}, {...|  1|1.123123123123|Michael|      {black, brown}|       {6}|[[Java, Scala, C+...|
   * |{{NewY1231ork, N1...|[{SanJos123123e, ...|  2|2.123123123123|   Test|      {black, brown}|       {6}|[[Java, Scala, C+...|
   * |                null|[{SanJose, CA}, {...|  3|3.123123123123|   Test|      {black, brown}|       {6}|[[Java, Scala, C+...|
   * |{{NewYork, NY}, {...|[{LA, CA}, {Sandi...|  4|4.123123123123|   John|{bla3221ck, b12rown}|     {633}|     [[Spark, Java]]|
   * |{{Haha, PA}, {nul...|[{Pittsburgh, PA}...|  5|5.123123123123|  Jonas|      {black, black}|       {7}|[[Java, Scala, C+...|
   * +--------------------+--------------------+---+--------------+-------+--------------------+----------+--------------------+
   * </pre>
   */
  private Dataset<Row> nestedDataframe() {
    final String row1 =
        "{\"name\":\"Michael\",\"addresses\":[{\"city\":\"SanJose\",\"state\":\"CA\"},{\"city\":\"Sandiago\",\"state\":\"CA\"}],"
            + "\"address_nested\":{\"current\":{\"state\":\"NY\",\"city\":\"NewYork\"},\"previous\":{\"state\":\"NJ\",\"city\":\"Newark\"}},"
            + "\"properties\":{\"hair\":\"brown\",\"eye\":\"black\"},\"secondProp\":{\"height\":\"6\"},\"subjects\":[[\"Java\",\"Scala\",\"C++\"],"
            + "[\"Spark\",\"Java\"]],\"id\":1,\"magic_number\":1.123123123123}";
    final String row2 =
        "{\"name\":\"Test\",\"addresses\":[{\"city\":\"SanJos123123e\",\"state\":\"CA\"},{\"city\":\"Sand12312iago\",\"state\":\"CA\"}],"
            + "\"address_nested\":{\"current\":{\"state\":\"N12Y\",\"city\":\"NewY1231ork\"}},\"properties\":{\"hair\":\"brown\",\"eye\":\"black\"},"
            + "\"secondProp\":{\"height\":\"6\"},\"subjects\":[[\"Java\",\"Scala\",\"C++\"],[\"Spark\",\"Java\"]],\"id\":2,\"magic_number\":2.123123123123}";
    final String row3 =
        "{\"name\":\"Test\",\"addresses\":[{\"city\":\"SanJose\",\"state\":\"CA\"},{\"city\":\"Sandiago\",\"state\":\"CA\"}],"
            + "\"properties\":{\"hair\":\"brown\",\"eye\":\"black\"},\"secondProp\":{\"height\":\"6\"},\"subjects\":"
            + "[[\"Java\",\"Scala\",\"C++\"],[\"Spark\",\"Java\"]],\"id\":3,\"magic_number\":3.123123123123}";
    final String row4 =
        "{\"name\":\"John\",\"addresses\":[{\"city\":\"LA\",\"state\":\"CA\"},{\"city\":\"Sandiago\",\"state\":\"CA\"}],"
            + "\"address_nested\":{\"current\":{\"state\":\"NY\",\"city\":\"NewYork\"},\"previous\":{\"state\":\"NJ123\"}},"
            + "\"properties\":{\"hair\":\"b12rown\",\"eye\":\"bla3221ck\"},\"secondProp\":{\"height\":\"633\"},\"subjects\":"
            + "[[\"Spark\",\"Java\"]],\"id\":4,\"magic_number\":4.123123123123}";
    final String row5 =
        "{\"name\":\"Jonas\",\"addresses\":[{\"city\":\"Pittsburgh\",\"state\":\"PA\"},{\"city\":\"Sandiago\",\"state\":\"CA\"}],"
            + "\"address_nested\":{\"current\":{\"state\":\"PA\",\"city\":\"Haha\"},\"previous\":{\"state\":\"NJ\"}},"
            + "\"properties\":{\"hair\":\"black\",\"eye\":\"black\"},\"secondProp\":{\"height\":\"7\"},\"subjects\":[[\"Java\",\"Scala\",\"C++\"],"
            + "[\"Spark\",\"Java\"]],\"id\":5,\"magic_number\":5.123123123123}";

    List<String> jsonList = Lists.newArrayList();
    jsonList.add(row1);
    jsonList.add(row2);
    jsonList.add(row3);
    jsonList.add(row4);
    jsonList.add(row5);
    JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    SQLContext sqlContext = new SQLContext(javaSparkContext);
    JavaRDD<String> rdd = javaSparkContext.parallelize(jsonList);

    return sqlContext.read().json(rdd);
  }
}
