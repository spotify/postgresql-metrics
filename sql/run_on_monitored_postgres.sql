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
#
# -----
#
# pg_stat_repl is used to get replication statistics by a non-superuser.
#
# Run the following SQL as super user, and grant access to your metrics user separately.
#
# Please use your own passwords and usernames on the script below,
# do not run it as is, unless you do not care about security regarding metrics user.
#

CREATE ROLE metricsuser WITH PASSWORD 'secret' LOGIN;

CREATE OR REPLACE FUNCTION pg_stat_repl()
RETURNS SETOF pg_catalog.pg_stat_replication AS $$
BEGIN
  RETURN QUERY(SELECT * FROM pg_catalog.pg_stat_replication);
END$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE VIEW public.pg_stat_repl AS SELECT * FROM pg_stat_repl();

GRANT SELECT ON public.pg_stat_repl TO metricsuser;

CREATE EXTENSION IF NOT EXISTS pgstattuple;

CREATE OR REPLACE FUNCTION pgstattuple_for_table_oid(BIGINT)
  RETURNS TABLE (current_database NAME, table_len BIGINT, tuple_count BIGINT,
  tuple_len BIGINT, tuple_percent FLOAT, dead_tuple_count BIGINT,
  dead_tuple_len BIGINT, dead_tuple_percent FLOAT, free_space BIGINT,
  free_percent FLOAT) AS $$
BEGIN
  RETURN QUERY(SELECT current_database(), * FROM pgstattuple($1));
END$$ LANGUAGE plpgsql SECURITY DEFINER;

GRANT EXECUTE ON FUNCTION pgstattuple_for_table_oid(BIGINT) TO metricsuser;
