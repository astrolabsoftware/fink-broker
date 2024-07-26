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
from pyspark.sql import SparkSession

import logging
from logging import Logger

from fink_broker.tester import spark_unit_tests


# Used for reverse compatibility
# Should be removed and everything should use root logger as recommended by logging module maintener: https://stackoverflow.com/a/2631396/2784039
def get_fink_logger(name: str, log_level: str = "INFO") -> Logger:
    return init_logger(log_level)


def init_logger(log_level: str = "INFO") -> logging.Logger:
    """Initialise python root logger. Suitable for both driver and executors.

    Parameters
    ----------
    log_level : str
        Minimum level of log wanted: DEBUG, INFO, WARNING, ERROR, CRITICAL, OFF

    Returns
    -------
    logger : logging.Logger
        Python Logger

    Examples
    --------
    >>> log = init_logger("INFO")
    >>> log.info("Hi!")
    """
    # Format of the log message to be printed
    FORMAT = "%(asctime)-15s "
    FORMAT += "%(levelname)s "
    FORMAT += "%(funcName)s "
    FORMAT += "(%(filename)s "
    FORMAT += "line %(lineno)d): "
    FORMAT += "%(message)s"

    # Date format
    DATEFORMAT = "%y/%m/%d %H:%M:%S"

    logger = logging.getLogger()

    # Set the minimum log level
    logger.setLevel(log_level)

    streamHandler = logging.StreamHandler()
    formatter = logging.Formatter(FORMAT, DATEFORMAT)
    streamHandler.setFormatter(formatter)
    logger.addHandler(streamHandler)

    return logger


def inspect_application(logger):
    """Print INFO and DEBUG statements about the current application

    Parameters
    ----------
    logger : log4j logger
        Logger initialised by get_fink_logger.

    Examples
    --------
    >>> log = get_fink_logger(__name__, "DEBUG")
    >>> inspect_application(log) # doctest: +SKIP
    """
    if logger.isEnabledFor(logging.DEBUG):
        spark = SparkSession.builder.getOrCreate()

        logger.debug("Application started")
        logger.debug("Python version: {}".format(spark.sparkContext.pythonVer))
        logger.debug("Spark version: {}".format(spark.sparkContext.version))

        # Debug statements
        conf = "\n".join([str(i) for i in spark.sparkContext.getConf().getAll()])
        logger.debug(conf)


if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """
    globs = globals()
    # Run the Spark test suite
    spark_unit_tests(globs, withstreaming=False)
