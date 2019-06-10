# Welcome to Fink's documentation!

## Overview

Fink is a broker infrastructure enabling a wide range of applications and services to connect to large streams of alerts issued from telescopes all over the world. Fink core is based on the [Apache Spark](http://spark.apache.org/) framework, and more specifically it uses the [Structured Streaming processing engine](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html). The language chosen for the API is Python, which is widely used in the astronomy community, has a large scientific ecosystem and easily connects with existing tools.

Fink's goal is twofold: providing a robust infrastructure and state-of-the-art streaming services to LSST scientists, and enabling other science cases in a big data context. Fink decouples resources needed for listening to the stream (online, critical), and resources used for services: scalable, robust, and modular!

## Rationale

The design of Fink is driven by three pillars:

* **Simplicity:** a broker should be simple enough to be used by a majority of scientists and maintained in real-time. This means the exposed API must be easily understood by anyone, and the code base should be as small as possible to allow easy maintenance and upgrade.
* **Scalability:** broker's behaviour should be the same regardless the amount of incoming data. This implies the technology used for this is scalable.
* **Flexibility:** the broker structure should allow for easy extension. As data will come, new features will be added, and the broker should be able to incorporate those smoothly. In addition, the broker should be able to connect to a large numbers of external tools and frameworks to maximize its scientific production without redeveloping tools.

We want Fink to be able to _filter, aggregate, enrich, consume_ incoming Kafka topics (stream of alerts) or otherwise _transform_ into new topics for further consumption or follow-up processing. Following LSST [LDM-612](https://github.com/lsst/LDM-612), Fink's ultimate objectives are (no specific order):

* redistributing alert packets
* filtering alerts
* cross-correlating alerts with other static catalogs or alert stream
* classifying events scientifically
* providing user interfaces to the data
* coordinating scientific activity among collaborators
* triggering followup observing
* for users with appropriate data rights, facilitating followup queries and/or user-generated processing within the corresponding Data Access Center
* managing annotation & citation as followup observations are made
* collecting classification and other information gathered by the scientific community

## Installation

You need Python 3.6+, Apache Spark 2.4+, and docker-compose (latest) installed.
Define `SPARK_HOME`  as per your Spark installation (typically, `/usr/local/spark`) and add the path to the Spark binaries in `.bash_profile`:

```bash
# in ~/.bash_profile
# as per your spark installation directory (eg. /usr/local/spark)
export SPARK_HOME=/usr/local/spark
export SPARKLIB=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.7-src.zip
export PYTHONPATH=${SPARKLIB}:$PYTHONPATH
export PATH=${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}
```
Then execute the following (to ensure working of coverage module) :

```bash
echo "spark.yarn.jars=${SPARK_HOME}/jars/*.jar" >> ${SPARK_HOME}/conf/spark-defaults.conf
echo "spark.python.daemon.module coverage_daemon" >> ${SPARK_HOME}/conf/spark-defaults.conf
```

Set the path to HBase
```bash
# in ~/.bash_profile
# as per your hbase installation directory (eg. /usr/local/hbase)
export HBASE_HOME=/usr/local/hbase
export PATH=$PATH:$HBASE_HOME/bin
```

Clone the repository:

```bash
git clone https://github.com/astrolabsoftware/fink-broker.git
cd fink-broker
```

Then install the required python dependencies:

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

Finally, define `FINK_HOME` and add the path to the Fink binaries and modules in your `.bash_profile` (assuming you are using `bash`...):

```bash
# in ~/.bash_profile
export FINK_HOME=/path/to/fink-broker
export PYTHONPATH=$FINK_HOME:$PYTHONPATH
export PATH=$FINK_HOME/bin:$PATH
```

Both the [dashboard](user_guide/dashboard.md) and the [simulator](user_guide/simulator.md) rely on docker-compose.


## Getting started

The repository contains some alerts from the ZTF experiment required for the test suite in the folder `datasim`. If you need to download more alerts data, go to the datasim directory and execute the download script:

```bash
cd datasim
./download_ztf_alert_data.sh
cd ..
```

Make sure the test suite is running fine. Just execute:

```bash
fink_test [--without-integration] [-h]
```

You should see plenty of Spark logs (and yet we have shut most of them!), but no failures hopefully! Success is silent, and the coverage is printed on screen at the end. You can disable integration tests by specifying the argument `--without-integration`. Then let's test some functionalities of Fink by simulating a stream of alert, and monitoring it via the dashboard. Start the dashboard, and go to `http://localhost:5000`:
```bash
fink start dashboard
# Creating dashboardnet_website_1 ... done
# Dashboard served at http://localhost:5000
```

Connect the monitoring service to the stream:
```bash
fink start checkstream --simulator > live.log &
```

Send a small burst of alerts:
```bash
fink start simulator
```
Now go to `http://localhost:5000/live.html` and see the alerts coming! The dashboard
should refresh automatically, but do it manually in case it does not work.

You can easily see the running services by using:

```bash
fink show
1 Fink service(s) running:
USER               PID  %CPU %MEM      VSZ    RSS   TT  STAT STARTED      TIME COMMAND
julien           61200   0.0  0.0  4277816   1232 s001  S     8:20am   0:00.01 /bin/bash /path/to/fink start checkstream --simulator
Use <fink stop service_name> to stop a service.
Use <fink start dashboard> to start the dashboard or check its status.
```

Finally stop monitoring and shut down the UI simply using:
```bash
fink stop checkstream
fink stop dashboard
```

To get help about `fink`, just type:

```shell
fink
Handle Kafka stream received by Apache Spark

 Usage:
 	to start: fink start <service> [-h] [-c <conf>] [--simulator]
 	to stop : fink stop <service> [-h] [-c <conf>]

 To get this help:
 	fink

 To get help for a service:
 	fink start <service> -h

 To see the running processes:
  fink show

 Available services are: dashboard, checkstream, stream2raw, raw2science, distribution
 Typical configuration would be ${FINK_HOME}/conf/fink.conf
```
