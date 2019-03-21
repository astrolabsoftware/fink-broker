import os
import doctest
import numpy as np

def regular_unit_tests(global_args=None, verbose=False):
    """ Base commands for the regular unit test suite

    Include this routine in the main of a module, and execute:
    python3 mymodule.py
    to run the tests.

    It should exit gracefully if no error (exit code 0),
    otherwise it will print on the screen the failure.

    Parameters
    ----------
    global_args: dict, optional
        Dictionary containing user-defined variables to
        be passed to the test suite. Default is None.
    verbose: bool
        If True, print useful debug messages.
        Default is False.

    Examples
    ----------
    Set "toto" to "myvalue", such that it can be used during tests:
    >>> globs = globals()
    >>> globs["toto"] = "myvalue"
    >>> regular_unit_tests(global_args=globs)
    """
    if global_args is None:
        global_args = globals()

    # Numpy introduced non-backward compatible change from v1.14.
    if np.__version__ >= "1.14.0":
        np.set_printoptions(legacy="1.13")

    doctest.testmod(globs=global_args, verbose=verbose)

def spark_unit_tests(global_args=None, verbose=False, withstreaming=False):
    """ Base commands for the regular unit test suite

    Include this routine in the main of a module, and execute:
    python3 mymodule.py
    to run the tests.

    It should exit gracefully if no error (exit code 0),
    otherwise it will print on the screen the failure.

    Parameters
    ----------
    global_args: dict, optional
        Dictionary containing user-defined variables to
        be passed to the test suite. Default is None.
    verbose: bool
        If True, print useful debug messages.
        Default is False.

    Examples
    ----------
    Set "toto" to "myvalue", such that it can be used during tests:
    >>> globs = globals()
    >>> globs["toto"] = "myvalue"
    >>> regular_unit_tests(global_args=globs)
    """
    if global_args is None:
        global_args = globals()

    from pyspark.sql import SparkSession
    from pyspark import SparkConf

    conf = SparkConf()
    confdic = {
        "spark.jars.packages": os.environ["FINK_PACKAGES"],
        "spark.python.daemon.module": "coverage_daemon"}
    conf.setMaster("local[2]")
    conf.setAppName("test")
    for k, v in confdic.items():
        conf.set(key=k, value=v)
    spark = SparkSession\
        .builder\
        .appName("test")\
        .config(conf=conf)\
        .getOrCreate()

    global_args["spark"] = spark

    if withstreaming:
        dfstream = spark.readStream.format("kafka")\
            .option("kafka.bootstrap.servers", os.environ["KAFKA_IPPORT_SIM"])\
            .option("subscribe", os.environ["KAFKA_TOPIC_SIM"])\
            .option("startingOffsets", "earliest").load()
        global_args["dfstream"] = dfstream

    # Numpy introduced non-backward compatible change from v1.14.
    if np.__version__ >= "1.14.0":
        np.set_printoptions(legacy="1.13")

    doctest.testmod(globs=global_args, verbose=verbose)
