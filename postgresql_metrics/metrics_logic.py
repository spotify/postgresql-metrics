#!/usr/bin/env python
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
This module contains the CLI and the logic for gathering and
forwarding Postgres metrics into a metrics gatherer using Spotify FFWD.

https://github.com/spotify/ffwd

Notice that the REAMDE.md in the repository root contains short descriptions
of all the Python modules within this project.
"""
import argparse
import json
import os
import socket
import time

from postgresql_metrics import metrics_gatherer
from postgresql_metrics.postgres_queries import (
    get_db_name_from_connection,
    get_db_connection,
    get_major_version
)
from postgresql_metrics.prepare_db import prepare_databases_for_metrics
from postgresql_metrics.common import (
    init_logging_file,
    init_logging_stderr,
    init_logging_syslog,
    get_logger,
    find_and_parse_config
)

LOG = get_logger()

DEFAULT_CONFIG_PATH = "/etc/postgresql-metrics/postgresql-metrics.yml"


# LAST_RUN_TIMES_FOR_STATS is a dict of dicts, i.e. key pointing to a key pointing to a value:
# database name -> stats function pointer -> last run timestamp
# This means that we have separate last run timestamps per database and per stats function.
LAST_RUN_TIMES_FOR_STATS = {}

DEFAULT_FFWD_PORT = 19000
DEFAULT_FFWD_HOST = '127.0.0.1'


def push_to_ffwd(metric_dicts, ffwd_addr, data_formatter=json.dumps):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        for metric in metric_dicts:
            data = data_formatter(metric)
            LOG.debug('send UDP packet to {} with data:\n{}', ffwd_addr, data)
            s.sendto(bytes(data, 'UTF-8'), ffwd_addr)
    finally:
        s.close()


def _is_time_to_call_stats_func_and_update_ts(database_name, metrics_func, run_interval_sec):
    """Check if it is time to schedule new metrics gathering call,
    and assume the call will be made immediately if yes.
    This means that the call timestamp for the given database_name and metrics_func
    is updated within this function.
    """
    last_run_timestamp = LAST_RUN_TIMES_FOR_STATS.get(database_name, {}).get(metrics_func, 0)
    if time.time() - last_run_timestamp > run_interval_sec:
        if database_name not in LAST_RUN_TIMES_FOR_STATS:
            LAST_RUN_TIMES_FOR_STATS[database_name] = {}
        LAST_RUN_TIMES_FOR_STATS[database_name][metrics_func] = time.time()
        return True
    return False


def _call_all_db_functions(db_stats_functions, db_parameters, schedule=False, db_name=None):
    """Iterates through all given statistics functions, calling them with the given parameter.
    The db_parameter can be a database connection or a file path to Postgres data directory,
    depending on the statistics function to call.
    """
    metrics = []
    for (db_metrics_func, interval_s) in db_stats_functions:
        if schedule:
            if not db_name:
                # DB name is given only if we want to make database specific scheduling.
                db_name = "__cluster_global__"
            is_call_required = \
                _is_time_to_call_stats_func_and_update_ts(db_name, db_metrics_func, interval_s)
        else:
            is_call_required = True
        if is_call_required:
            try:
                LOG.debug('calling stats function {}', db_metrics_func.__name__)
                metrics.extend(db_metrics_func(*db_parameters))
            except Exception:
                LOG.exception('failed calling stats function: ' + db_metrics_func.__name__)
    return metrics


def get_stats_functions_from_conf(func_key_name, conf):
    """Finds the statistics function configured, and ensures that the callables
    are found from metrics_gatherer.py."""
    stats_functions = []
    if func_key_name in conf and conf[func_key_name] is not None:
        for func_name, call_interval in conf[func_key_name]:
            stats_func = getattr(metrics_gatherer, func_name)
            if not stats_func or not callable(stats_func):
                raise Exception("statistics function '" + func_name +
                                "' not found in configuration under key name: " + func_key_name)
            stats_functions.append((stats_func, int(call_interval)))
    return stats_functions


def get_all_stats_functions_from_conf(conf):
    db_functions = get_stats_functions_from_conf('db_functions', conf)
    global_db_functions = get_stats_functions_from_conf('global_db_functions', conf)
    # `data_dir_functions` is deprecated, but to preserve backwards compatibility still read
    data_dir_functions = get_stats_functions_from_conf('data_dir_functions', conf)
    if data_dir_functions:
        LOG.warn("data_dir_functions field in config is deprecated -- consider moving functions to global_db_functions")
    all_global_db_functions = data_dir_functions + global_db_functions
    return db_functions, all_global_db_functions


def get_all_metrics_now(db_connections, conf):
    """Get all the metrics immediately without any scheduling.
    First gets the global stats with first available database connection,
    and then gets the rest per database.
    """
    db_functions, global_db_functions = get_all_stats_functions_from_conf(conf)
    data_dir = figure_out_postgres_data_dir(db_connections[0], conf)

    all_metrics = _call_all_db_functions(global_db_functions, (data_dir, db_connections[0]))
    for db_connection in db_connections:
        all_metrics.extend(_call_all_db_functions(db_functions, (data_dir, db_connection)))
    return all_metrics


def get_all_metrics_scheduled(db_connections, conf):
    """Get all the metrics in scheduled manner, not calling all the functions every time.
    First gets the global stats with first available database connection,
    and then gets the rest per database.
    """
    db_functions, global_db_functions = get_all_stats_functions_from_conf(conf)
    data_dir = figure_out_postgres_data_dir(db_connections[0], conf)

    all_metrics = _call_all_db_functions(global_db_functions, (data_dir, db_connections[0]), schedule=True)
    for db_connection in db_connections:
        db_name = get_db_name_from_connection(db_connection)
        all_metrics.extend(_call_all_db_functions(db_functions, (data_dir, db_connection),
                                                  schedule=True, db_name=db_name))
    return all_metrics


def run_long_running_ffwd(conf):
    db_connections = get_db_connections_with_conf(conf)
    ffwd_address = (DEFAULT_FFWD_HOST, DEFAULT_FFWD_PORT)
    if conf and conf.get('ffwd'):
        ffwd_address = (conf['ffwd'].get('host', DEFAULT_FFWD_HOST),
                        int(conf['ffwd'].get('port', DEFAULT_FFWD_PORT)))
    try:
        LOG.info("starting a long running statistics polling loop with {} database(s)",
                 len(db_connections))
        while True:
            try:
                # Notice that the scheduling is separate from this few second sleep,
                # but as the granularity is in tens of seconds, few seconds interval is enough.
                time.sleep(5.0)
                db_connections = confirm_connections_work(conf, db_connections)
                metrics = get_all_metrics_scheduled(db_connections, conf)
                if metrics:
                    LOG.info("sending {} metrics to ffwd...", len(metrics))
                    push_to_ffwd(metrics, ffwd_address)
            except (KeyboardInterrupt, SystemExit):
                LOG.warn('*** keyboard interrupt / system exit ***')
                raise
            except Exception:
                LOG.exception('metrics check failed')
    finally:
        for db_connection in db_connections:
            if not db_connection.closed:
                db_connection.close()


def confirm_connections_work(conf, db_connections):
    """Call this to confirm that all connections are still alive before using them.
    Will recreate any closed connections."""
    confirmed_connections = []
    for db_connection in db_connections:
        if db_connection.closed:
            db_name = get_db_name_from_connection(db_connection)
            LOG.warn("database connection is closed to db '{}', reconnecting", db_name)
            confirmed_connections.append(connect_to_single_db_with_conf(conf, db_name))
        else:
            confirmed_connections.append(db_connection)
    return confirmed_connections


def connect_to_single_db_with_conf(conf, database_name):
    LOG.info("open database connection to {}:{}, user '{}', database '{}'",
             conf['postgres']['host'], conf['postgres']['port'],
             conf['postgres']['user'], database_name)
    return get_db_connection(database_name,
                             conf['postgres']['user'],
                             conf['postgres']['password'],
                             host=conf['postgres']['host'],
                             port=int(conf['postgres']['port']))


def get_db_connections_with_conf(conf):
    connections = []
    if 'databases' in conf['postgres']:
        if not conf['postgres']['databases']:
            raise Exception("no target databases defined in configuration")
        for database_name in conf['postgres']['databases']:
            connections.append(connect_to_single_db_with_conf(conf, database_name))
    elif 'database' in conf['postgres']:
        # this is here just for backward compatibility, before the databases option handled above
        connections.append(connect_to_single_db_with_conf(conf, conf['postgres']['database']))
    if not connections:
        raise Exception("could not connect to database with configuration:\n" + str(conf))
    return connections


def figure_out_postgres_data_dir(db_connection, conf):
    data_dir = conf['postgres']['data_dir']
    if not data_dir:
        db_version = get_major_version(db_connection)
        data_dir = "/var/lib/postgresql/{0}/main".format(db_version)
    if not os.path.isdir(data_dir):
        LOG.debug("data directory '{}' doesn't exist", data_dir)
        data_dir = None
    else:
        LOG.debug('using postgres data directory: {}', data_dir)
    return data_dir


DESCRIPTION = """Spotify PostgreSQL Metrics
This tool fetches metrics from a Postgres database cluster,
and returns the results in Metrics 2.0 compatible JSON format.

You can run the 'long-running-ffwd' as a background process that keeps
sending the gathered metrics into FFWD as configured, or you can call
this CLI tool directly for simply printing out the metrics for other
purposes.

Run the prepare-db command to prepare your monitored databases in
the Postgres cluster for the statistics gathering. You need to run
the prepare-db command with database super-user credentials.
"""

USAGE = """Usage: postgresql-metrics <command>

<command> can be:
    all                Show all available metrics
    long-running-ffwd  Run in infinite loop, sending metrics to FFWD
    prepare-db         Create required users, extensions, and functions for metrics.
"""


def main():
    parser = argparse.ArgumentParser(description=DESCRIPTION, usage=USAGE)
    parser.add_argument("command", help="the command to run")
    parser.add_argument("-c", "--config-path", default=DEFAULT_CONFIG_PATH,
                        help="configuration path, checks also folder 'default' on given path [{}]"
                        .format(DEFAULT_CONFIG_PATH))

    args = parser.parse_args()

    conf = find_and_parse_config(args.config_path)
    if 'postgres' not in conf:
        raise Exception("failed parsing configuration from: " + args.config_path)
    log_level = conf.get('log', {}).get('log_level', 'debug')

    if args.command == 'all':
        init_logging_stderr(log_level)
        db_connections = get_db_connections_with_conf(conf)
        get_all_metrics_now(db_connections, conf)
        print("# sleep 5 s to get diffs on derivative metrics")
        time.sleep(5.0)
        for metric in get_all_metrics_now(db_connections, conf):
            print(metric)

    elif args.command == 'long-running-ffwd':
        if conf['log']['log_to_stderr'] is True:
            init_logging_stderr(log_level)
        if conf['log']['log_to_file'] is True:
            init_logging_file(conf['log']['filename'], log_level,
                              conf['log']['rotate_file_log'], conf['log']['file_rotate_max_size'])
        if conf['log']['log_to_syslog'] is True:
            init_logging_syslog(log_level, facility=conf['log']['syslog_facility'])
        run_long_running_ffwd(conf)

    elif args.command == 'prepare-db':
        init_logging_stderr(log_level)
        prepare_databases_for_metrics(conf)


if __name__ == '__main__':  # if this script is called from command line
    main()
