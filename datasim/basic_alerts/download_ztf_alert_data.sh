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
set -e

# Download subset of ZTF public data - 22 MB (zipped)
fn=ztf_public_20190903.tar.gz
wget --no-check-certificate https://ztf.uw.edu/alerts/public/${fn}

# Untar the alert data - 55 MB
tar -zxvf ${fn}
rm ${fn}
