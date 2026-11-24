#!/bin/bash
# Copyright 2019-2026 AstroLab Software
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

pssh -p 12 -t 100000000 \
    -o /tmp/python_ad_out/ \
    -e /tmp/python_ad_err/ \
    -h /localhome/julien.peloton/fink-broker/conf/ztf/spark_ips 'rm /tmp/forest_*.onnx'
