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
This module contains code for preparing each monitored database in the cluster
for the functionality required in postgresql-metrics project.

This includes creating an appropriate metrics user, enabling required extensions,
and creating required functions and views.
"""
import getpass

import psycopg2
from psycopg2 import sql

from postgresql_metrics.postgres_queries import get_db_connection
from postgresql_metrics.common import get_logger

LOG = get_logger("postgresql-metrics-prepare-db")

REPLICATION_STATS_VIEW = 'public.pg_stat_repl'
PGSTATTUPLES_FUNC_NAME = 'pgstattuple_for_table_oid'
PGSTATTUPLES_FUNC = PGSTATTUPLES_FUNC_NAME + '(BIGINT)'

PGVERSION_WAL_RECEIVER = 90600
INCOMING_REPLICATION_STATS_VIEW = "stat_incoming_replication"


def query_user_for_superuser_credentials():
    username = input("Provide a Postgres role name with superuser privileges "
                     "in the configured cluster: ")
    password = getpass.getpass("Give the password: ")
    return username, password


def connect_as_super_user(db_name, conf):
    db_connection = None
    try:
        db_connection = psycopg2.connect(database=db_name)
        db_connection.autocommit = True
    except psycopg2.OperationalError:
        LOG.info("could not connect as local superuser with current user, credentials required")

    if not db_connection:
        superuser, password = query_user_for_superuser_credentials()
        db_connection = get_db_connection(db_name, superuser, password,
                                          host=conf['postgres']['host'],
                                          port=int(conf['postgres']['port']))

    if not db_connection or db_connection.closed:
        raise Exception("failed connecting the database: " + db_name)

    return db_connection


def check_if_database_is_slave(db_connection):
    """Returns True if the queried database is a slave node,
    i.e. is in recovery mode streaming data from master.
    """
    with db_connection.cursor() as c:
        c.execute("SELECT pg_is_in_recovery()")
        result = c.fetchone()
        return bool(result) and result[0]


def check_if_role_exists(db_connection, role_name):
    with db_connection.cursor() as c:
        c.execute("SELECT rolname FROM pg_roles WHERE rolname=%s", [role_name])
        result = c.fetchone()
        return bool(result) and result[0] == role_name


def check_if_role_has_db_privilege(db_connection, role_name, db_name, privilege):
    with db_connection.cursor() as c:
        c.execute("SELECT * FROM has_database_privilege(%s, %s, %s)",
                  [role_name, db_name, privilege])
        result = c.fetchone()
        return bool(result) and result[0]


def check_if_role_has_table_privilege(db_connection, role_name, table_name, privilege):
    with db_connection.cursor() as c:
        c.execute("SELECT * FROM has_table_privilege(%s, %s, %s)",
                  [role_name, table_name, privilege])
        result = c.fetchone()
        return bool(result) and result[0]


def check_if_role_has_function_privilege(db_connection, role_name, function_name, privilege):
    with db_connection.cursor() as c:
        c.execute("SELECT * FROM has_function_privilege(%s, %s, %s)",
                  [role_name, function_name, privilege])
        result = c.fetchone()
        return bool(result) and result[0]


def check_if_replication_stats_view_exists(db_connection):
    with db_connection.cursor() as c:
        c.execute("SELECT table_name FROM information_schema.tables "
                  "WHERE table_name='pg_stat_repl'")
        result = c.fetchone()
        return bool(result) and result[0] == 'pg_stat_repl'


def check_if_pgstattuples_extension_exists(db_connection):
    with db_connection.cursor() as c:
        c.execute("SELECT proname FROM pg_proc WHERE proname=%s", [PGSTATTUPLES_FUNC_NAME])
        result = c.fetchone()
        return bool(result) and result[0] == 'pgstattuples'


def create_role_with_login(db_connection, metrics_user, metrics_user_password):
    LOG.info("creating role '{}' with login privilege", metrics_user)
    with db_connection.cursor() as c:
        c.execute("CREATE ROLE " + metrics_user + " WITH PASSWORD %s LOGIN;",
                  [metrics_user_password])


def create_replication_stats_view(db_connection):
    LOG.info("creating view {}", REPLICATION_STATS_VIEW)
    sql = """CREATE OR REPLACE FUNCTION public.pg_stat_repl()
RETURNS SETOF pg_catalog.pg_stat_replication AS $$
BEGIN
RETURN QUERY(SELECT * FROM pg_catalog.pg_stat_replication);
END$$ LANGUAGE plpgsql SECURITY DEFINER;"""
    with db_connection.cursor() as c:
        c.execute(sql)
        c.execute("CREATE VIEW " + REPLICATION_STATS_VIEW
                  + " AS SELECT * FROM public.pg_stat_repl()")

def create_pgstattuples_extension(db_connection):
    LOG.info("creating extension pgstattuple with access function {}", PGSTATTUPLES_FUNC)
    sql = "CREATE OR REPLACE FUNCTION " + PGSTATTUPLES_FUNC + """
RETURNS TABLE (current_database NAME, table_len BIGINT, tuple_count BIGINT,
               tuple_len BIGINT, tuple_percent FLOAT, dead_tuple_count BIGINT,
               dead_tuple_len BIGINT, dead_tuple_percent FLOAT, free_space BIGINT,
               free_percent FLOAT) AS $$
BEGIN
  RETURN QUERY(SELECT current_database(), * FROM pgstattuple($1));
END$$ LANGUAGE plpgsql SECURITY DEFINER;"""
    with db_connection.cursor() as c:
        c.execute("CREATE EXTENSION IF NOT EXISTS pgstattuple;")
        c.execute(sql)

def check_if_incoming_replication_status_view_exists(db_connection):
    with db_connection.cursor() as c:
        c.execute("SELECT table_name FROM information_schema.tables "
                  "WHERE table_name=%s", (INCOMING_REPLICATION_STATS_VIEW,))
        result = c.fetchone()
        return bool(result) and result[0] == INCOMING_REPLICATION_STATS_VIEW

def create_incoming_replication_status_view(db_connection):
    LOG.info("creating view {}", INCOMING_REPLICATION_STATS_VIEW)
    func_sql = """CREATE OR REPLACE FUNCTION public.stat_incoming_replication()
RETURNS SETOF pg_catalog.pg_stat_wal_receiver AS $$
BEGIN
RETURN QUERY(SELECT * FROM pg_catalog.pg_stat_wal_receiver);
END$$ LANGUAGE plpgsql SECURITY DEFINER;"""
    view_sql = "CREATE OR REPLACE VIEW public.{0} AS SELECT * FROM {0}()".format(
        INCOMING_REPLICATION_STATS_VIEW)
    with db_connection.cursor() as c:
        c.execute(func_sql)
        c.execute(view_sql)

def prepare_databases_for_metrics(conf):
    """Tries first to connect to localhost database as default user,
    which works if the local user is setup as local Postgres superuser.
    If this fails, queries for Postgres superuser credentials.
    """
    metrics_user = conf['postgres']['user']
    metrics_user_password = conf['postgres']['password']
    LOG.info("prepare databases for metrics user '{}'", metrics_user)

    db_names = []
    if 'databases' in conf['postgres']:
        db_names = conf['postgres']['databases']
    elif 'database' in conf['postgres']:
        db_names = [conf['postgres']['database']]

    for db_name in db_names:
        LOG.info("connecting to database '{}' as super user", db_name)
        db_connection = connect_as_super_user(db_name, conf)

        if check_if_database_is_slave(db_connection):
            LOG.info("database is a slave, run prepare-db on master")
            break

        if not check_if_role_exists(db_connection, metrics_user):
            create_role_with_login(db_connection, metrics_user, metrics_user_password)
        else:
            LOG.info("role already exists: {}", metrics_user)

        if not check_if_role_has_db_privilege(db_connection, metrics_user, db_name, 'connect'):
            LOG.info("grant connect privilege to user '{}' for database: {}",
                     metrics_user, db_name)
            with db_connection.cursor() as c:
                c.execute("GRANT CONNECT ON database " + db_name + " TO " + metrics_user)
        else:
            LOG.info("role '{}' already has connect privilege to database: {}",
                     metrics_user, db_name)

        if not check_if_replication_stats_view_exists(db_connection):
            create_replication_stats_view(db_connection)
        else:
            LOG.info("replication stats view already exists")

        if not check_if_role_has_table_privilege(db_connection, metrics_user,
                                                 REPLICATION_STATS_VIEW, 'select'):
            LOG.info("grant select privilege to user '{}' for relation: {}",
                     metrics_user, REPLICATION_STATS_VIEW)
            with db_connection.cursor() as c:
                c.execute("GRANT SELECT ON " + REPLICATION_STATS_VIEW + " TO " + metrics_user)
        else:
            LOG.info("role '{}' already has select privilege to relation: {}",
                     metrics_user, REPLICATION_STATS_VIEW)

        if not check_if_pgstattuples_extension_exists(db_connection):
            create_pgstattuples_extension(db_connection)
        else:
            LOG.info("pgstattuples extension already exists")

        if not check_if_role_has_function_privilege(db_connection, metrics_user,
                                                    PGSTATTUPLES_FUNC, 'execute'):
            LOG.info("grant execute privilege to user '{}' for function: {}",
                     metrics_user, PGSTATTUPLES_FUNC)
            with db_connection.cursor() as c:
                c.execute("GRANT EXECUTE ON FUNCTION " + PGSTATTUPLES_FUNC + " TO "
                          + metrics_user)
        else:
            LOG.info("role '{}' already has execute privilege to function: {}",
                     metrics_user, PGSTATTUPLES_FUNC)

        if db_connection.server_version >= PGVERSION_WAL_RECEIVER:
            if not check_if_incoming_replication_status_view_exists(db_connection):
                create_incoming_replication_status_view(db_connection)
            else:
                LOG.info("incoming replication status view already exists")

            if not check_if_role_has_table_privilege(db_connection, metrics_user,
                                                     INCOMING_REPLICATION_STATS_VIEW, 'select'):
                LOG.info("grant select privilege to user '{}' for relation: {}",
                         metrics_user, INCOMING_REPLICATION_STATS_VIEW)
                with db_connection.cursor() as c:
                    c.execute(sql.SQL("GRANT SELECT ON {} TO {}").format(
                        sql.Identifier(INCOMING_REPLICATION_STATS_VIEW),
                        sql.Identifier(metrics_user)))
            else:
                LOG.info("role '{}' already has select privilege to relation: {}",
                         metrics_user, REPLICATION_STATS_VIEW)
        else:
            LOG.info("skipping setup for incoming replication view, requires Postgres version >= %s",
                     PGVERSION_WAL_RECEIVER)

        LOG.info("database '{}' prepared for metrics user: {}", db_name, metrics_user)
