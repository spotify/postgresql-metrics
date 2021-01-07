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
This module defines some basic common application functionality, like logging.
"""

import os

import logbook
import yaml


def get_logger(logger_name="postgresql-metrics"):
    return logbook.Logger(logger_name)


def figure_out_log_level(given_level):
    if isinstance(given_level, str):
        return logbook.lookup_level(given_level.strip().upper())
    else:
        return given_level


def init_logging_stderr(log_level='notset', bubble=False):
    handler = logbook.StderrHandler(level=figure_out_log_level(log_level), bubble=bubble)
    handler.push_application()
    get_logger().debug("stderr logging initialized")


def init_logging_file(filename, log_level='notset', rotate_log=True, rotate_max_size=10485760,
                      bubble=True):
    log_dir = os.path.dirname(filename)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    if rotate_log is True:
        handler = logbook.RotatingFileHandler(filename, level=figure_out_log_level(log_level),
                                              max_size=int(rotate_max_size), bubble=bubble)
    else:
        handler = logbook.FileHandler(filename, level=figure_out_log_level(log_level),
                                      bubble=bubble)
    handler.push_application()
    get_logger().debug("file based logging initialized in directory: " + log_dir)


def init_logging_syslog(log_level='notset', facility='local0', bubble=True):
    handler = logbook.SyslogHandler('postgresql-metrics', facility=facility,
                                    level=figure_out_log_level(log_level), bubble=bubble)
    handler.push_application()
    get_logger().debug("syslog logging initialized")


def merge_configs(to_be_merged, default):
    """Merges two configuration dictionaries by overwriting values with
    same keys, with the priority on values given on the 'left' side, so
    the to_be_merged dict.

    Notice that with lists in the configuration, it skips from the default
    (right side) the tuples in that which already exist in the left side
    to_be_merged list. This is used to be able to override time intervals for
    default values in the configuration.

    Example:
    In [1]: x = [["get_stats_disk_usage_for_database", 180],
                 ["get_stats_tx_rate_for_database", 500]]
    In [2]: y = [["get_stats_seconds_since_last_vacuum_per_table", 60],
                 ["get_stats_tx_rate_for_database", 60]]
    In [3]: merge_configs(x, y)
    Out[3]:
    [['get_stats_disk_usage_for_database', 180],
     ['get_stats_tx_rate_for_database', 500],
     ['get_stats_seconds_since_last_vacuum_per_table', 60]]
    """
    if isinstance(to_be_merged, dict) and isinstance(default, dict):
        for k, v in default.items():
            if k not in to_be_merged:
                to_be_merged[k] = v
            else:
                to_be_merged[k] = merge_configs(to_be_merged[k], v)
    elif isinstance(to_be_merged, list) and isinstance(default, list):
        same_keys = set()
        for x in to_be_merged:
            for y in default:
                if isinstance(x, (list, set, tuple)) and isinstance(y, (list, set, tuple)) and len(
                        x) > 0 and len(y) > 0 and x[0] == y[0]:
                    same_keys.add(x[0])
        for y in default:
            if not isinstance(y, (list, set, tuple)) or y[0] not in same_keys:
                to_be_merged.append(y)
    return to_be_merged


def find_and_parse_config(config_path):
    """Finds the service configuration file and parses it.
    Checks also a directory called default, to check for default configuration values,
    that will be overwritten by the actual configuration found on given path.
    """
    config_filename = os.path.basename(config_path)
    config_root = os.path.dirname(config_path)
    default_root = os.path.join(config_root, 'default')
    config_dict = {}
    for config_dir in (default_root, config_root):
        current_path = os.path.join(config_dir, config_filename)
        if os.path.isfile(current_path):
            with open(current_path, 'r') as f:
                read_config_dict = yaml.safe_load(f)
            config_dict = merge_configs(read_config_dict, config_dict)
    return config_dict
