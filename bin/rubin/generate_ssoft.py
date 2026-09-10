#!/usr/bin/env python
# Copyright 2023-2026 AstroLab Software
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
"""Construct the Rubin Solar System Object Fink Table (SSOFT)."""

from fink_broker.common.ssoft import generate_ssoft

if __name__ == "__main__":
    generate_ssoft(sso_file="sso_rubin_lc_aggregated.parquet")
