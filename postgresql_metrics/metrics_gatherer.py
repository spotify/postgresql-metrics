# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
This module provides all the metrics in correct format for output.

Steps for adding statistic values as metrics:
1) Write function for extracting the statistics value:
    * extract from DB, then write it in postgres_queries.py
    * extract from local Postgres data directory,
      then write it in localhost_postgres_stats.py
2) Write a function into default_metrics.py for transferring a statistic into a metric.
3) Write a function in this module to call both of the above defined functions,
   returning the metrics in correct form (always in a list, see below).
4) Add the name of the function in this module into the configuration,
   and define the interval the metric should be called.
"""

from postgresql_metrics.default_metrics import (
    metric_client_connections,
    metric_database_size,
    metric_transaction_rate,
    metric_rollbacks_rate,
    metric_seconds_since_last_vacuum,
    metric_blocks_read_from_disk,
    metric_blocks_read_from_buffer,
    metric_blocks_heap_hit_ratio,
    metric_locks_granted,
    metric_locks_waiting,
    metric_sec_since_oldest_xact_start,
    metric_table_bloat,
    metric_index_hit_ratio,
    metric_replication_delay_bytes,
    metric_wal_file_amount,
)

from postgresql_metrics.localhost_postgres_stats import get_amount_of_wal_files

from postgresql_metrics.postgres_queries import (
    get_client_connections_amount,
    get_disk_usage_for_database,
    get_transaction_rate_for_database,
    get_seconds_since_last_vacuum_per_table,
    get_heap_hit_statistics,
    get_lock_statistics,
    get_oldest_transaction_timestamp,
    get_table_bloat,
    get_index_hit_rates,
    get_replication_delays,
    get_tables_with_oids_for_current_db,
)


# Notice that all functions here are expected to return a list of metrics.
# Notice also that the names of these functions should match the configuration.

def get_stats_client_connections(db_connection):
    client_amount = get_client_connections_amount(db_connection)
    return [metric_client_connections(client_amount)]


def get_stats_disk_usage_for_database(db_connection):
    db_size = get_disk_usage_for_database(db_connection)
    return [metric_database_size(db_size[0], db_size[1])]


def get_stats_tx_rate_for_database(db_connection):
    db_name, tx_rate, tx_rollbacks = get_transaction_rate_for_database(db_connection)
    if tx_rate is not None:
        return [metric_transaction_rate(db_name, tx_rate),
                metric_rollbacks_rate(db_name, tx_rollbacks)]
    else:
        return []


def get_stats_seconds_since_last_vacuum_per_table(db_connection):
    last_vacuums_data = get_seconds_since_last_vacuum_per_table(db_connection)
    metrics = []
    for db_name, table_name, seconds_since in last_vacuums_data:
        metrics.append(metric_seconds_since_last_vacuum(db_name, table_name, seconds_since))
    return metrics


def get_stats_heap_hit_statistics(db_connection):
    db_name, heap_read, heap_hit, heap_hit_ratio = get_heap_hit_statistics(db_connection)
    metrics = []
    if heap_hit_ratio is not None:
        metrics.append(metric_blocks_read_from_disk(db_name, heap_read))
        metrics.append(metric_blocks_read_from_buffer(db_name, heap_hit))
        metrics.append(metric_blocks_heap_hit_ratio(db_name, heap_hit_ratio))
    return metrics


def get_stats_lock_statistics(db_connection):
    locks_by_type, [total_locks_waiting, total_locks_granted] = get_lock_statistics(db_connection)
    metrics = []
    for lock_type, [locks_waiting, locks_granted] in locks_by_type.iteritems():
        metrics.append(metric_locks_granted(lock_type, locks_granted))
        metrics.append(metric_locks_waiting(lock_type, locks_waiting))
    metrics.append(metric_locks_granted("total", total_locks_granted))
    metrics.append(metric_locks_waiting("total", total_locks_waiting))
    return metrics


def get_stats_oldest_transaction_timestamp(db_connection):
    db_name, sec_since_oldest_xact_start = get_oldest_transaction_timestamp(db_connection)
    metrics = []
    if sec_since_oldest_xact_start is not None:
        metrics.append(metric_sec_since_oldest_xact_start(db_name, sec_since_oldest_xact_start))
    return metrics


def get_stats_table_bloat(db_connection):
    tables_with_oids = get_tables_with_oids_for_current_db(db_connection)
    metrics = []
    for table_oid, table_name in tables_with_oids:
        db_name, table_bloat_percentage = get_table_bloat(db_connection, table_oid)
        if db_name:
            metrics.append(metric_table_bloat(db_name, table_name, table_bloat_percentage))
    return metrics


def get_stats_index_hit_rates(db_connection):
    index_hit_rates = get_index_hit_rates(db_connection)
    metrics = []
    for db_name, table_name, index_hit_ratio in index_hit_rates:
        if index_hit_ratio is not None:
            metrics.append(metric_index_hit_ratio(db_name, table_name, index_hit_ratio))
    return metrics


def get_stats_replication_delays(db_connection):
    replication_delays = get_replication_delays(db_connection)
    metrics = []
    for client_addr, delay_in_bytes in replication_delays:
        metrics.append(metric_replication_delay_bytes(client_addr, delay_in_bytes))
    return metrics


def get_stats_wal_file_amount(data_dir):
    return [metric_wal_file_amount(get_amount_of_wal_files(data_dir))]
