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
from pip.req import parse_requirements
from setuptools import find_packages
from setuptools import setup

# parse_requirements() returns generator of pip.req.InstallRequirement objects
install_requirements = parse_requirements('requirements.txt')

# requirements below is a list of requirements:
#   e.g. ['psycopg2==2.6.1', 'logbook==0.10.1']
requirements = [str(ir.req) for ir in install_requirements]

setup(name='postgresql-metrics',
      version='0.2.3',
      author=u'Hannu Varjoranta',
      author_email='hannu.varjoranta@spotify.com',
      url='https://github.com/spotify/postgresql-metrics',
      description='Simple service to provide metrics for your PostgreSQL database',
      packages=find_packages(),
      install_requires=requirements,
      entry_points={
          'console_scripts': [
              'postgresql-metrics=postgresql_metrics.metrics_logic:main',
          ]}
      )
