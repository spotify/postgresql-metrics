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
This module defines the default output format for the gathered metrics.

This metrics format follows the Metrics 2.0 conventions:
http://metrics20.org/
"""

DEFAULT_METRIC_NAMESPACE = 'postgresql'
DEFAULT_METRIC_TYPE = 'metric'


def create_default_metric(value, attributes=None):
    metric = {
        'type': DEFAULT_METRIC_TYPE,
        'key': DEFAULT_METRIC_NAMESPACE,
        'value': value,
    }
    if attributes:
        metric['attributes'] = dict(attributes)
    return metric


def metric_client_connections(value):
    return create_default_metric(value,
                                 {'what': 'client-connections',
                                  'unit': 'connection'})


def metric_database_size(database_name, value):
    return create_default_metric(value,
                                 {'what': 'database-size',
                                  'database': database_name,
                                  'unit': 'B'})


def metric_transaction_rate(database_name, value):
    return create_default_metric(value,
                                 {'what': 'transaction-rate',
                                  'type': 'transactions',
                                  'database': database_name,
                                  'unit': 'transaction'})


def metric_rollbacks_rate(database_name, value):
    return create_default_metric(value,
                                 {'what': 'transaction-rollbacks',
                                  'type': 'transactions',
                                  'database': database_name,
                                  'unit': 'transaction'})


def metric_seconds_since_last_vacuum(database_name, table_name, value):
    return create_default_metric(value,
                                 {'what': 'last-vacuum',
                                  'database': database_name,
                                  'table': table_name,
                                  'unit': 's'})


def metric_blocks_read_from_disk(database_name, value):
    return create_default_metric(float(value),
                                 {'what': 'blocks-read-from-disk',
                                  'type': 'heap-reads',
                                  'database': database_name,
                                  'unit': 'blocks'})


def metric_blocks_read_from_buffer(database_name, value):
    return create_default_metric(float(value),
                                 {'what': 'blocks-read-from-buffer',
                                  'type': 'heap-reads',
                                  'database': database_name,
                                  'unit': 'blocks'})


def metric_blocks_heap_hit_ratio(database_name, value):
    return create_default_metric(float(value),
                                 {'what': 'blocks-heap-hit-ratio',
                                  'database': database_name,
                                  'unit': 'buffer_hit%'})


def metric_locks_granted(locktype, value):
    return create_default_metric(value,
                                 {'what': 'locks_granted',
                                  'type': 'locks',
                                  'locktype': locktype,
                                  'unit': 'lock'})


def metric_locks_waiting(locktype, value):
    return create_default_metric(value,
                                 {'what': 'locks_waiting',
                                  'type': 'locks',
                                  'locktype': locktype,
                                  'unit': 'lock'})


def metric_sec_since_oldest_xact_start(database_name, value):
    return create_default_metric(value,
                                 {'what': 'sec-since-oldest-xact-start',
                                  'database': database_name,
                                  'unit': 's'})


def metric_xid_remaining_ratio(value):
    return create_default_metric(value,
                                 {'what': 'xid-remaining',
                                  'unit': '%'})


def metric_multixact_remaining_ratio(value):
    return create_default_metric(value,
                                 {'what': 'mxid-remaining',
                                  'unit': '%'})


def metric_multixact_members_per_mxid(value):
    return create_default_metric(value,
                                 {'what': 'multixact-members-per-mxid',
                                  'unit': 'members/id'})


def metric_multixact_members_remaining_ratio(value):
    return create_default_metric(value,
                                 {'what': 'multixact-members-remaining',
                                  'unit': '%'})

def metric_wal_file_amount(value):
    return create_default_metric(value,
                                 {'what': 'wal-file-amount',
                                  'unit': 'file'})


def metric_table_bloat(database_name, table_name, value):
    return create_default_metric(float(value),
                                 {'what': 'table-bloat',
                                  'database': database_name,
                                  'table': table_name,
                                  'unit': 'bloat%'})


def metric_index_hit_ratio(database_name, table_name, value):
    return create_default_metric(float(value),
                                 {'what': 'index-hit',
                                  'database': database_name,
                                  'table': table_name,
                                  'unit': 'index_hit%'})


def metric_replication_delay_bytes(client_addr, value):
    return create_default_metric(value,
                                 {'what': 'replication-delay-bytes',
                                  'slave': client_addr,
                                  'unit': 'B'})


def metric_incoming_replication_running(replication_host, value):
    return create_default_metric(value,
                                 {'what': 'incoming-replication-running',
                                  'master': replication_host,
                                  'unit': 'msg'})
