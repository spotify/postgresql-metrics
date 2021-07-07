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
This module contains code for statistics extraction that is based
on having a connection to a Postgres database, and running queries through it.
"""

import psycopg2
import re

from postgresql_metrics.common import get_logger

LOG = get_logger()


# contains mappings of metric-name: (last_timestamp, last_value)
# used to derive metric value diffs between the current and the previous value
DERIVE_DICT = dict()

# regex used to extra host from conninfo string
CONNINFO_HOST_RE = re.compile(r'($|\s)host=(?P<host>.*?)(^|\s)')


def get_db_connection(database, username, password, host='127.0.0.1', port=5432,
                      connect_timeout=10):
    connection = psycopg2.connect(user=username,
                                  password=password,
                                  host=host,
                                  port=int(port),
                                  database=database,
                                  connect_timeout=connect_timeout)
    connection.autocommit = True
    return connection


def get_db_name_from_connection(connection):
    """example dsn: dbname=varjodb user=varjo password=xxxxxxxx host=127.0.0.1
    This works also for closed connection.
    """
    for dsn_part in connection.dsn.split():
        key, value = dsn_part.split('=')
        if key.strip() == 'dbname':
            return value.strip()
    return None


def get_metric_diff(db_name, metric_name, current_time, current_value):
    derive_dict_key = db_name + "_" + metric_name
    diff = None
    if derive_dict_key in DERIVE_DICT:
        last_time, last_value = DERIVE_DICT[derive_dict_key]
        seconds_since_last_check = int((current_time - last_time).total_seconds())
        if seconds_since_last_check == 0:
            diff = 0
        else:
            diff = float(current_value - last_value) / seconds_since_last_check
    DERIVE_DICT[derive_dict_key] = (current_time, current_value)
    return diff


def query(cursor, sql, params=None):
    """accepts a database connection or cursor"""
    if type(cursor) == psycopg2._psycopg.connection:
        cursor = cursor.cursor()
    LOG.debug('QUERY "{}" {}', sql, params)
    try:
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        results = cursor.fetchall()
    except Exception:
        LOG.exception("failed calling the database")
        results = []
    LOG.debug('QUERY RESULT: {}', results)
    return results


def get_tables_with_oids_for_current_db(conn):
    tables = []
    results = query(conn,
                    "SELECT oid, relname FROM pg_class WHERE relkind = 'r' "
                    "AND relname NOT LIKE 'pg_%' AND relname NOT LIKE 'sql_%'")
    for result in results:
        table_oid, table_name = result
        tables.append((table_oid, table_name))
    return tables


def get_client_connections_amount(conn):
    results = query(conn, 'SELECT count(*) FROM pg_stat_activity')
    if results:
        return results[0][0]
    return None


def get_disk_usage_for_database(conn):
    sql = ("SELECT datname, pg_database_size(datname) FROM pg_database "
           "WHERE datname = current_database()")
    results = query(conn, sql)
    if results:
        return results[0]
    return None


def get_major_version(conn):
    """Get the major version part of the PostgreSQL version, i.e. the first two digits"""
    results = query(conn, "SELECT substring(version() from $$(\d+\.\d+)\.\d+$$)")
    if results:
        return results[0][0]
    return None


def get_transaction_rate_for_database(conn):
    sql = ("SELECT current_database(), datname, now(), xact_commit + xact_rollback, xact_rollback "
           "FROM pg_stat_database WHERE datname = current_database()")
    results = query(conn, sql)
    db_name, dat_name, time_now, transactions_now, rollbacks_now = results[0]
    if None in results[0]:
        LOG.error("Fetching transactions got 'None' in result set")
        return None, None, None
    recent_transactions = get_metric_diff(db_name, 'transactions', time_now, transactions_now)
    recent_rollbacks = get_metric_diff(db_name, 'rollbacks', time_now, rollbacks_now)
    return dat_name, recent_transactions, recent_rollbacks


def get_seconds_since_last_vacuum_per_table(conn):
    """Returns a list of tuples: (db_name, table_name, seconds_since_last_vacuum)
    where seconds_since_last_vacuum is 0 if no vacuum is done ever (stays flat zero)"""
    sql = ("SELECT current_database(), relname, now(), last_vacuum, last_autovacuum "
           "FROM pg_stat_user_tables")
    results = query(conn, sql)
    table_last_vacuum_list = []
    for db_name, table_name, time_now, last_vacuum, last_autovacuum in results:
        latest_vacuum = None
        if last_vacuum or last_autovacuum:
            latest_vacuum = max([x for x in (last_vacuum, last_autovacuum) if x])
        seconds_since_last_vacuum = int((time_now - (latest_vacuum or time_now)).total_seconds())
        table_last_vacuum_list.append((db_name, table_name, seconds_since_last_vacuum))
    return table_last_vacuum_list


def get_heap_hit_statistics(conn):
    sql = ("SELECT current_database(), now(), sum(heap_blks_read), sum(heap_blks_hit) "
           "FROM pg_statio_user_tables")
    results = query(conn, sql)
    if not results or None in results[0]:
        LOG.error("fetching heap_hit_statistics got empty results: {}", str(results))
        return None, None, None, None
    db_name, time_now, heap_read_now, heap_hit_now = results[0]
    recent_heap_read = get_metric_diff(db_name, 'heap_read', time_now, heap_read_now)
    recent_heap_hit = get_metric_diff(db_name, 'heap_hit', time_now, heap_hit_now)
    recent_heap_hit_ratio = None
    if recent_heap_read is not None:
        if recent_heap_hit == 0:
            recent_heap_hit_ratio = 0
        else:
            recent_heap_hit_ratio = recent_heap_hit / float(recent_heap_hit + recent_heap_read)
    return db_name, recent_heap_read, recent_heap_hit, recent_heap_hit_ratio


def get_lock_statistics(conn):
    sql = ("SELECT locktype, granted, count(*) FROM pg_locks GROUP BY locktype, granted")
    results = query(conn, sql)
    total = [0, 0]
    lock_stats = {}
    for lock_type, granted, count in results:
        if lock_type not in lock_stats:
            lock_stats[lock_type] = [0, 0]
        lock_stats[lock_type][granted] = count
        total[granted] += count
    return [lock_stats, total]


def get_oldest_transaction_timestamp(conn):
    sql = ("SELECT datname, now(), xact_start FROM pg_stat_activity "
           "WHERE xact_start IS NOT NULL AND datname=current_database() "
           "ORDER BY xact_start ASC LIMIT 1")
    results = query(conn, sql)
    if results:
        db_name, time_now, xact_start = results[0]
        seconds_since_oldest_xact_start = int((time_now - (xact_start or time_now)).total_seconds())
        return db_name, seconds_since_oldest_xact_start
    return None, None


def get_max_mxid_age(conn):
    # `mxid_age` is only available on postgres 9.5 and newer
    if conn.server_version < 95000:
        LOG.error("Unable to check mxid_age on versions of postgres below 9.5")
        return None
    sql = "SELECT max(mxid_age(relminmxid)) FROM pg_class WHERE relminmxid <> '0'"
    results = query(conn, sql)
    if not results:
        return None
    mxid_age, = results[0]
    return int(mxid_age)


def get_max_xid_age(conn):
    sql = "SELECT max(age(datfrozenxid)) FROM pg_database"
    results = query(conn, sql)
    if not results:
        return None
    xid_age, = results[0]
    return int(xid_age)


def get_replication_delays(conn):
    sql = ("SELECT client_addr, "
           "pg_xlog_location_diff(pg_current_xlog_location(), replay_location) AS bytes_diff "
           "FROM public.pg_stat_repl")
    if is_in_recovery(conn):
        # pg_current_xlog_location cannot be called in a replica
        # use pg_last_xlog_receive_location for monitoring cascade replication
        sql = sql.replace("pg_current_xlog_location", "pg_last_xlog_receive_location")
    if conn.server_version >= 100000: # PostgreSQL 10 and higher
        sql = sql.replace('_xlog', '_wal')
        sql = sql.replace('_location', '_lsn')
    all_delays = []
    results = query(conn, sql)
    for result_row in results:
        client_addr = result_row[0]
        bytes_diff = int(result_row[1])
        all_delays.append((client_addr, bytes_diff))
    return all_delays


def get_table_bloat(conn, table_oid):
    """Based on extension pgstattuple, so you need to call CREATE EXTENSION before using this.
    Check the function get_tables_with_oids_for_current_db to see how to get table oids.
    """
    results = query(conn, "SELECT current_database, dead_tuple_percent "
                          "FROM pgstattuple_for_table_oid(%s)", [table_oid])
    if results:
        db_name, dead_tuple_percent = results[0]
        return db_name, dead_tuple_percent / 100.0
    return None, None


def get_index_hit_rates(conn):
    sql = ("SELECT current_database() as db_name, relname as table_name, "
           "idx_scan as index_hit, seq_scan as index_miss "
           "FROM pg_stat_user_tables")
    results = query(conn, sql)
    index_hit_rates = []
    LOG.debug(results)
    for db_name, table_name, index_hit, index_miss in results:
        if index_hit is not None and index_miss is not None:
            if index_hit == 0:
                recent_ratio = 0
            else:
                recent_ratio = index_hit / float(index_miss + index_hit)
            index_hit_rates.append((db_name, table_name, recent_ratio))
        else:
            index_hit_rates.append((db_name, table_name, None))
    return index_hit_rates


def get_wal_receiver_status(conn):
    sql = ("SELECT conninfo, CASE WHEN status = 'streaming' THEN 1 ELSE 0 END "
           "FROM public.stat_incoming_replication")
    results = query(conn, sql)
    host_replication_status = []
    for conn_info, status in results:
        host = CONNINFO_HOST_RE.search(conn_info).groupdict().get('host', 'UNKNOWN')
        host_replication_status.append((host, status))
    return host_replication_status


def is_in_recovery(conn):
    return query(conn, "SELECT pg_is_in_recovery()")[0][0]
