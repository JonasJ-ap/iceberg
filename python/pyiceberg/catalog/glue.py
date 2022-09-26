#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
from __future__ import annotations

import boto3

from typing import Union, Optional, List, Set, Dict

from pyiceberg.catalog import Catalog, PropertiesUpdateSummary
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.partitioning import PartitionSpec, UNPARTITIONED_PARTITION_SPEC
from pyiceberg.table.sorting import SortOrder, UNSORTED_SORT_ORDER, SortDirection
from pyiceberg.typedef import Identifier, Properties, EMPTY_DICT


class GlueCatalog(Catalog):

    def __init__(self, name: str, properties: Properties):
        self.client = boto3.client("glue")
        super().__init__(name, properties)

    def _check_response(self, response: Dict[str, Dict[str, str]]):
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise ValueError(f"Got unexpected status code {response['HttpStatusCode']}")

    def create_table(
        self,
        identifier: str | Identifier,
        schema: Schema,
        location: str | None = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
    ) -> Table:
        pass

    def load_table(self, identifier: str | Identifier) -> Table:
        database_name, table_name = self.identifier_to_tuple(identifier)

        response = self.client.get_table(DatabseName=database_name, Name=table_name)

        self._check_response(response)
        # TODO: metadata stuff
        return None

    def drop_table(self, identifier: str | Identifier) -> None:
        database_name, table_name = self.identifier_to_tuple(identifier)
        self.client.delete_table(
            DatabaseName=database_name,
            Name=table_name
        )
        return None

    def purge_table(self, identifier: str | Identifier) -> None:
        self.drop_table(identifier)

    def rename_table(self, from_identifier: str | Identifier, to_identifier: str | Identifier) -> Table:
        raise NotImplementedError("AWS Glue does not support renaming of tables")

    def create_namespace(self, namespace: str | Identifier, properties: Properties = EMPTY_DICT) -> None:
        identifier = self.identifier_to_tuple(namespace)
        database_name, = identifier

        response = self.client.create_database(
            DatabaseInput={
                'Name': database_name,
                #                'Description': 'string',
                #                'LocationUri': 'string',
                'Parameters': properties
            }
        )
        self._check_response(response)

    def drop_namespace(self, namespace: str | Identifier) -> None:
        identifier = self.identifier_to_tuple(namespace)
        database_name, = identifier

        response = self.client.delete_database(
            Name=database_name
        )

        self._check_response(response)

    def list_tables(self, namespace: str | Identifier) -> list[Identifier]:
        identifier = self.identifier_to_tuple(namespace)
        database_name, = identifier

        catalog_id = self._get_database(database_name)['CatalogId']

        tables = self.client.get_tables(
            CatalogId=catalog_id,
            DatabaseName=database_name
        )['TableList']

        return [
            identifier + self.identifier_to_tuple(table['Name']) for table in tables
        ]

    def list_namespaces(self, namespace: str | Identifier = ()) -> list[Identifier]:
        databases = self.client.get_databases()['DatabaseList']
        return [
            self.identifier_to_tuple(database['Name']) for database in databases
        ]

    def load_namespace_properties(self, namespace: str | Identifier) -> Properties:
        identifier = self.identifier_to_tuple(namespace)
        database_name, = identifier
        return self._get_database(database_name)['Parameters']

    def update_namespace_properties(
        self, namespace: str | Identifier, removals: set[str] | None = None, updates: Properties = EMPTY_DICT
    ) -> PropertiesUpdateSummary:
        identifier = self.identifier_to_tuple(namespace)
        database_name, = identifier
        database = self._get_database(database_name)
        parameters = database['Parameters']

        removed: Set[str] = set()
        updated: Set[str] = set()

        if removals:
            for key in removals:
                if key in parameters:
                    parameters[key] = None
                    removed.add(key)
        if updates:
            for key, value in updates.items():
                parameters[key] = value
                updated.add(key)

        expected_to_change = (removals or set()).difference(removed)

        response = self.client.update_database(
            CatalogId=database['CatalogId'],
            Name=database['Name'],
            DatabaseInput={
                'Name': database['Name'],
                # 'Description': database.get('Description', ''),
                # 'LocationUri': database.get('LocationUri', ''),
                'Parameters': parameters
            }
        )

        self._check_response(response)

        return PropertiesUpdateSummary(
            removed=list(removed or []), updated=list(updates.keys() if updates else []),
            missing=list(expected_to_change)
        )

    def _get_database(self, database_name: str):
        databases = self.client.get_databases()['DatabaseList']
        return next(database for database in databases if database['Name'] == database_name)


