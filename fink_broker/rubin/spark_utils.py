# Copyright 2019-2025 AstroLab Software
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
import logging
from pyspark.sql import SparkSession


from fink_broker.common.tester import spark_unit_tests

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_LOG = logging.getLogger(__name__)


def get_schema_from_parquet(filename):
    """Extract schema from dataframe column `lsst_alert_schema`

    Parameters
    ----------
    filename: str
        Parquet filename containing alert data

    Returns
    -------
    major: int
        Major version
    minor: int
        Minor version
    """
    spark = SparkSession.builder.getOrCreate()

    schema_version = (
        spark.read
        .format("parquet")
        .load(filename)
        .select("lsst_schema_version")
        .limit(1)
        .collect()[0][0]
    )
    major_version, minor_version = schema_version.split("lsst.v")[1].split("_")
    return int(major_version), int(minor_version)


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    globs = globals()

    # Run the Spark test suite
    spark_unit_tests(globs, withstreaming=False)
