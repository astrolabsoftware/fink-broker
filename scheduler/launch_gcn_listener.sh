#!/bin/bash
# Copyright 2019-2024 AstroLab Software
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

# Check FINK_MM path exists
if [[ -z "$FINK_MM_HOME" ]]; then
	echo "You did not set FINK_MM_HOME."
	echo "The program will not be launched."
	echo "Exiting."
	exit
fi

mkdir -p $HOME/fink_mm_out

# should be up 24/7
nohup fink_mm gcn_stream start --config $FINK_MM_HOME/fink_mm/conf/fink_mm.conf --restart > $HOME/fink_mm_out/gcn_stream.log &
