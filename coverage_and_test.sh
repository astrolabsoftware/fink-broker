#!/bin/bash
# Copyright 2018 AstroLab Software
# Author: Julien Peloton
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

## Script to launch the python test suite and measure the coverage.
## Must be launched as ./coverage_and_test.sh
set -e

export PYTHONPATH="${SPARK_HOME}/python/test_coverage:$PYTHONPATH"
export COVERAGE_PROCESS_START="$PWD/.coveragerc"

# Run the test suite on the modules
for i in python/fink_broker/*.py
do
  coverage run --source=. $i
done

# Combine individual reports
coverage combine

unset COVERAGE_PROCESS_START

## Print and store the report if machine related to julien
## Otherwise the result is sent to codecov (see .travis.yml)
isLocal=`whoami`
if [[ $isLocal = *"julien"* ]]
then
  coverage report

  echo " " >> cov.txt
  echo $(date) >> cov.txt
  coverage report >> cov.txt

  coverage html
  cd ../
fi
