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
from setuptools import find_packages
from setuptools import setup


def requirements(f):
    reqs = open(f, 'r').read().splitlines()
    reqs = [r for r in reqs if not r.strip().startswith('#')]
    return reqs


setup(name='postgresql-metrics',
      version='0.3.3',
      author=u'Hannu Varjoranta',
      author_email='hannu.varjoranta@spotify.com',
      url='https://github.com/spotify/postgresql-metrics',
      description='Simple service to provide metrics for your PostgreSQL database',
      packages=find_packages(),
      install_requires=requirements('requirements.txt'),
      entry_points={
          'console_scripts': [
              'postgresql-metrics=postgresql_metrics.metrics_logic:main',
          ]}
      )
