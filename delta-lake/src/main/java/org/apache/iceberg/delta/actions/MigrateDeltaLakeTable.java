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
package org.apache.iceberg.delta.actions;

import java.util.Map;
import org.apache.iceberg.actions.Action;

/** Migrates an existing Delta Lake table to Iceberg in place. */
public interface MigrateDeltaLakeTable
    extends Action<MigrateDeltaLakeTable, MigrateDeltaLakeTable.Result> {

  /**
   * Sets table properties in the newly created Iceberg table. Any properties with the same key name
   * will be overwritten.
   *
   * @param properties a map of properties to set
   * @return this for method chaining
   */
  MigrateDeltaLakeTable tableProperties(Map<String, String> properties);

  /** The action result that contains a summary of the execution. */
  interface Result {

    /** Returns the number of migrated data files. */
    long migratedDataFilesCount();
  }
}
