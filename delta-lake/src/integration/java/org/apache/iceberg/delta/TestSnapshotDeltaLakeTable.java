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
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.delta.catalog.DeltaCatalog;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class TestSnapshotDeltaLakeTable extends SparkDeltaLakeSnapshotTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestSnapshotDeltaLakeTable.class);
  private static final String NAMESPACE = "default";
  private String partitionedIdentifier;
  private String unpartitionedIdentifier;
  private static final String defaultSparkCatalog = "spark_catalog";
  private static final String icebergCatalogName = "iceberg_hive";

  @Parameterized.Parameters(name = "Catalog Name {0} - Options {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      new Object[] {
        icebergCatalogName,
        SparkSessionCatalog.class.getName(),
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

  @Rule public TemporaryFolder temp = new TemporaryFolder();
  @Rule public TemporaryFolder other = new TemporaryFolder();

  private final String partitionedTableName = "partitioned_table";
  private final String unpartitionedTableName = "unpartitioned_table";

  private String partitionedLocation;
  private String unpartitionedLocation;

  public TestSnapshotDeltaLakeTable(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
    spark.conf().set("spark.sql.catalog." + defaultSparkCatalog, DeltaCatalog.class.getName());
  }

  @Before
  public void before() {
    try {
      File partitionedFolder = temp.newFolder();
      File unpartitionedFolder = other.newFolder();
      partitionedLocation = partitionedFolder.toURI().toString();
      unpartitionedLocation = unpartitionedFolder.toURI().toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    partitionedIdentifier = destName(defaultSparkCatalog, partitionedTableName);
    unpartitionedIdentifier = destName(defaultSparkCatalog, unpartitionedTableName);

    spark.sql(String.format("DROP TABLE IF EXISTS %s", partitionedIdentifier));
    spark.sql(String.format("DROP TABLE IF EXISTS %s", unpartitionedIdentifier));

    // Create a partitioned and unpartitioned table, doing a few inserts on each
    IntStream.range(0, 3)
        .forEach(
            i -> {
              List<SimpleRecord> record =
                  Lists.newArrayList(new SimpleRecord(i, UUID.randomUUID().toString()));

              Dataset<Row> df = spark.createDataFrame(record, SimpleRecord.class);

              df.write()
                  .format("delta")
                  .mode(i == 0 ? SaveMode.Overwrite : SaveMode.Append)
                  .partitionBy("id")
                  .option("path", partitionedLocation)
                  .saveAsTable(partitionedIdentifier);

              df.write()
                  .format("delta")
                  .mode(i == 0 ? SaveMode.Overwrite : SaveMode.Append)
                  .option("path", unpartitionedLocation)
                  .saveAsTable(unpartitionedIdentifier);
            });

    // Delete a record from the table
    spark.sql("DELETE FROM " + partitionedIdentifier + " WHERE id=0");
    spark.sql("DELETE FROM " + unpartitionedIdentifier + " WHERE id=0");

    // Update a record
    spark.sql("UPDATE " + partitionedIdentifier + " SET id=3 WHERE id=1");
    spark.sql("UPDATE " + unpartitionedIdentifier + " SET id=3 WHERE id=1");
  }

  @After
  public void after() {
    // Drop the hive table.
    spark.sql(
        String.format(
            "DROP TABLE IF EXISTS %s", destName(defaultSparkCatalog, partitionedTableName)));
    spark.sql(
        String.format(
            "DROP TABLE IF EXISTS %s", destName(defaultSparkCatalog, unpartitionedTableName)));
  }

  @Test
  public void testMigratePartitioned() {
    // This will test the scenario that the user switches the configuration and sets the default
    // catalog to be Iceberg
    // AFTER they had made it Delta and written a delta table there
    DeltaLog deltaLog =
        DeltaLog.forTable(spark.sessionState().newHadoopConf(), partitionedLocation);

    String newTableIdentifier = destName(icebergCatalogName, "iceberg_table");
    SnapshotDeltaLakeTable.Result result =
        SnapshotDeltaLakeSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, partitionedLocation)
            .execute();

    checkSnapshotIntegrity(partitionedLocation, partitionedIdentifier, newTableIdentifier, result);
  }

  @Test
  public void testMigrateUnpartitioned() {
    // This will test the scenario that the user switches the configuration and sets the default
    // catalog to be Iceberg
    // AFTER they had made it Delta and written a delta table there
    DeltaLog deltaLog =
        DeltaLog.forTable(spark.sessionState().newHadoopConf(), unpartitionedLocation);

    String newTableIdentifier = destName(icebergCatalogName, "iceberg_table_unpartitioned");
    SnapshotDeltaLakeTable.Result result =
        SnapshotDeltaLakeSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, unpartitionedLocation)
            .execute();

    checkSnapshotIntegrity(
        unpartitionedLocation, unpartitionedIdentifier, newTableIdentifier, result);
  }

  private String destName(String catalogName, String dest) {
    if (catalogName.equals(defaultSparkCatalog)) {
      return NAMESPACE + "." + catalogName + "_" + dest;
    }
    return catalogName + "." + NAMESPACE + "." + catalogName + "_" + dest;
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

    Assert.assertEquals(
        "The original table and the transformed one should have the same size",
        deltaTableContents.size(),
        icebergTableContents.size());
    Assert.assertTrue(
        "The original table and the transformed one should have the same contents",
        icebergTableContents.containsAll(deltaTableContents));
    Assert.assertTrue(
        "The original table and the transformed one should have the same contents",
        deltaTableContents.containsAll(icebergTableContents));
    Assert.assertEquals(
        "The number of files in the delta table should be the same as the number of files in the snapshot iceberg table",
        deltaLog.update().getAllFiles().size(),
        snapshotReport.snapshotDataFilesCount());
  }
}
