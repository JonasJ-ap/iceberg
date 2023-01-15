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
package org.apache.iceberg.view;

import org.immutables.value.Value;

/**
 * View history entry.
 *
 * <p>An entry contains a change to the view state. At the given timestamp, the current version was
 * set to the given version ID.
 */
@Value.Immutable
public interface ViewHistoryEntry {
  /** Return the timestamp in milliseconds of the change */
  long timestampMillis();

  /** Return ID of the new current version */
  int versionId();
}
