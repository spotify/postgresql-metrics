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
on access to local Postgres data directory.
"""

import os
import re

from postgresql_metrics.common import get_logger

LOG = get_logger()


def get_multixact_member_files(data_dir):
    try:
        members_dir = os.path.join(data_dir, "pg_multixact", "members")
        if os.path.isdir(members_dir):
            return len([f for f in os.listdir(members_dir) if os.path.isfile(os.path.join(members_dir, f))])
        else:
            LOG.exception(f"Missing pg_multixact/members directory in data_dir: {data_dir}")
    except OSError:
        LOG.exception('Failed accessing multixact member files in: {data_dir}. Is data dir readable by user?')
    return 0


def get_amount_of_wal_files(data_dir):
    amount_of_wal_files = 0
    try:
        if data_dir and os.path.isdir(data_dir):
            wal_dir = os.path.join(data_dir, 'pg_wal')
            if not os.path.isdir(wal_dir):
                wal_dir = os.path.join(data_dir, 'pg_xlog')

            # each WAL file is named as 24-character hexadecimal number
            for possible_wal_file_name in os.listdir(wal_dir):
                if re.match('^[0-9A-F]{24}$', possible_wal_file_name):
                    amount_of_wal_files += 1
    except OSError:
        LOG.exception('Failed accessing WAL files. Is data dir readable by user?')
    return amount_of_wal_files
