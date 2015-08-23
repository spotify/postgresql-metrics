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
import types

import logbook
import yaml


def get_logger(logger_name="postgresql-metrics"):
    return logbook.Logger(logger_name)


def figure_out_log_level(given_level):
    if isinstance(given_level, types.StringTypes):
        return logbook.lookup_level(given_level.strip().upper())
    else:
        return given_level


def init_logging_stderr(log_level='notset', bubble=False):
    handler = logbook.StderrHandler(level=figure_out_log_level(log_level), bubble=bubble)
    handler.push_application()
    get_logger().debug("stderr logging initialized")


def init_logging_file(filename, log_level='notset', rotate_log=True, rotate_max_size=10485760):
    log_dir = os.path.dirname(filename)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    if rotate_log is True:
        handler = logbook.RotatingFileHandler(filename, level=figure_out_log_level(log_level),
                                              max_size=int(rotate_max_size), bubble=True)
    else:
        handler = logbook.FileHandler(filename, level=figure_out_log_level(log_level), bubble=True)
    handler.push_application()
    get_logger().debug("file based logging initialized in directory: " + log_dir)


def merge_configs(to_be_merged, default):
    """Merges two configuration dictionaries by overwriting values with same keys,
    with the priority on values given on the 'left' side, so the to_be_merged dict.
    """
    if isinstance(to_be_merged, dict) and isinstance(default, dict):
        for k, v in default.iteritems():
            if k not in to_be_merged:
                to_be_merged[k] = v
            else:
                to_be_merged[k] = merge_configs(to_be_merged[k], v)
    elif isinstance(to_be_merged, list) and isinstance(default, list):
        to_be_merged = default + to_be_merged
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
            with file(current_path, 'r') as f:
                read_config_dict = yaml.load(f)
            config_dict = merge_configs(read_config_dict, config_dict)
    return config_dict
