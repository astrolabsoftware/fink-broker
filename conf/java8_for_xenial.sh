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

# Allow Java 8 on xenial

# show current JAVA_HOME and java version
echo "JAVA_HOME: $JAVA_HOME"

# install Java 8
sudo add-apt-repository -y ppa:openjdk-r/ppa
sudo apt-get -qq update
sudo apt-get install -y openjdk-8-jdk --no-install-recommends
sudo update-java-alternatives -s java-1.8.0-openjdk-amd64

# change JAVA_HOME to Java 8
export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"

echo "JAVA_HOME: $JAVA_HOME"
