# Copyright 2019 AstroLab Software
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
from pyspark import SparkContext
from pyspark.sql import SparkSession

from fink_broker.tester import spark_unit_tests

def get_fink_logger(name: str = "test", log_level: str = "INFO"):
    """ Initialise log4j logger

    Parameters
    ----------
    name : str
        Name of the application to be logged. Typically __name__ of a
        function or module.
    log_level : str
        Minimum level of log wanted: INFO, WARN, ERROR, OFF, etc.

    Returns
    ----------
    logger : org.apache.log4j.Logger
        log4j Logger (Java object)

    Examples
    ----------
    >>> log = get_fink_logger(__name__, "INFO")
    >>> log.info("Hi!")
    """
    # Grab the running Spark context
    sc = SparkContext._active_spark_context

    # Get the logger
    loggermodule = sc._jvm.org.apache.log4j
    mylogger = loggermodule.Logger.getLogger(name)

    # Set the minimum level of log - INFO is the default
    level = getattr(loggermodule.Level, log_level, "INFO")

    loggermodule.LogManager.getLogger(name).setLevel(level)

    return mylogger

def inspect_application(logger):
    """Print INFO and DEBUG statements about the current application such
    as the Spark configuration, the Spark & Python versions.

    Parameters
    ----------
    logger : log4j logger
        Logger initialised by get_fink_logger.

    Examples
    -------
    >>> log = get_fink_logger(__name__, "DEBUG")
    >>> inspect_application(log) # doctest: +SKIP
    """
    spark = SparkSession.builder.getOrCreate()

    logger.info('Application started')
    logger.info('Python version: {}'.format(spark.sparkContext.pythonVer))
    logger.info('Spark version: {}'.format(spark.sparkContext.version))

    # Debug statements
    conf = "\n".join([str(i) for i in spark.sparkContext.getConf().getAll()])
    logger.debug(conf)


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """
    globs = globals()
    # Run the Spark test suite
    spark_unit_tests(globs, withstreaming=False)
