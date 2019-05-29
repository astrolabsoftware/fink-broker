# Testing Fink

Testing code is as important as the code itself, and in Fink we take it seriously.

## The need for testing, and challenges

Fink is based on the recent [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) module introduced in Spark 2.0, and the API is still changing rapidly with sometimes incompatibilities or deprecation happening. In Fink we want to make sure the code includes the latest features without breaking the usage, hence we perform intensive series of tests. There are unit tests in the code base, and integration tests making sure services can run each time the code is modified or dependencies change. In addition the test suite and code quality are constantly monitored with tools such as [sonarqube](https://www.sonarsource.com/), [codecov](https://codecov.io/), and [travis ci](https://travis-ci.org/).

The test suite must take into account that Fink is using distributing computing and works with streams. Therefore we develop a test environment that is able to:

- test the code in a distributed environment, and report code coverage on *all* players (driver and executors). This is achieved by running Spark inside all tests, and generating code coverage in headless mode using daemons to ensure full coverage.
- manipulate streams while testing the code. This is achieved using the [simulator](simulator.md) service.

## Configuration

To enable proper parallel execution of tests, that is coverage on both the driver and executors sides, you need to activate the coverage_daemon library ships with Spark. In a terminal, just execute:
```bash
echo "spark.python.daemon.module coverage_daemon" >> ${SPARK_HOME}/conf/spark-defaults.conf
```

## How to run the test suite

We provide a script to execute the test suite and report coverage of Fink code base:

```bash
fink_test [--without-integration] [-h]
Run the test suite of Fink

 Usage:
 	fink_test [--without-integration] [-h]

 By default, both unit tests and integration tests will be run.
 You can disable the integration tests by specifying --without-integration.
 You can specify the configuration file followed by -c or else the default (${FINK_HOME}/conf/fink.conf.travis) will be used.
 Use -h to display this help.
```

Both unit tests and integration tests will be run. You can disable the integration tests by specifying `--without-integration`. Then you should see plenty of verbose logs from Apache Spark at screen (and yet we have shut most of them!), `DeprecationWarning` that we need to handle at some point, and eventually failures will be printed out (success is silent) like:

```bash
**********************************************************************
File "python/fink_broker/avroUtils.py", line 103, in __main__.readschemafromavrofile
Failed example:
    print(schema['version'])
Expected:
    3.2
Got:
    3.1
```

If no failures, hooray, the code passes the test suite! Which obviously does not mean it is free of bugs, but that's a good start. At the end the coverage will be printed out like:

```bash
Name                                   Stmts   Miss  Cover
----------------------------------------------------------
bin/archive.py                            45      3    93%
bin/classify_fromstream.py                47      3    94%
bin/monitor_fromstream.py                 27      1    96%
bin/simulate_stream.py                    41      3    93%
python/__init__.py                         0      0   100%
python/fink_broker/__init__.py             0      0   100%
python/fink_broker/alertProducer.py       39      3    92%
python/fink_broker/avroUtils.py           24      0   100%
python/fink_broker/classification.py      51      3    94%
python/fink_broker/monitoring.py          37      2    95%
python/fink_broker/sparkUtils.py          22      0   100%
python/fink_broker/tester.py              27      2    93%
----------------------------------------------------------
TOTAL                                    360     20    94%
```

## Template for adding unit tests

All new code must be tested. For the code base (`python/fink_broker`) we use [doctest](https://docs.python.org/3/library/doctest.html). Newly introduced code must contain a part that will be processed by doctest to test the code and perform code coverage. This part is inside the docstrings of functions or classes:

```python
def my_new_function(arg: type_arg) -> return_type:
  """ A description

  Note: we love type hints.

  Parameters
  ----------
  arg: type_arg
    Description for `arg`

  Returns
  ----------
  out: return_type
    Description for `out`

  Examples
  ----------
  This example will be processed by doctest, to ensure the function
  is working as expected, and code coverage will be measured.
  >>> some_predictable_out = my_new_function(some_example_arg)
  >>> print(some_predictable_out)
  the_result_as_expected
  """
  # do something
  pass
```

Of course in practice it is not as easy to find relevant tests or capture all the possibilities - but we make our best efforts. To make the test suite easier to write and execute, Fink provides test helpers, especially to run tests with Spark:

```python
# Inside some module
from fink_broker.tester import spark_unit_tests

# your functions and classes go here
# Tests in docstrings can call the SparkSession `spark`,
# and manipulate streams from the simulators
# ...

if __name__ == "__main__":
    """ Execute the test suite with SparkSession initialised """

    # Run the Spark test suite
    spark_unit_tests(globals(), withstreaming=True)
```

Streams can be used also in tests (as shown above). just make sure you started the simulator before running the test suite:

```bash
# Publish few alerts at topic `zt-stream-sim`
fink start simulator -c conf/fink.conf.travis
fink_test
```

Note the configuration file `conf/fink.conf.travis` is currently used by default for running the test suite.

```bash
# in fink_test
...
source $FINK_HOME/conf/fink.conf.travis
...
```

A custom configuration file can be given as an argument `fink_test -c /path/to/custom.conf`
