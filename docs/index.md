# Welcome to Fink's documentation!

## Overview

Fink is a broker infrastructure using Apache Spark Streaming to receive and process alerts issued from telescopes all over the world. Fink core is based on the [Apache Spark](http://spark.apache.org/) framework, and more specifically its [streaming module](http://spark.apache.org/streaming/). The core of Fink is written in Python, which is widely used in the astronomy community, has a large scientific ecosystem and easily connects with existing tools.

Fink's goal is twofold: providing a robust infrastructure and state-of-the-art streaming services to LSST scientists, and enabling other science cases in a big data context. Fink implements the concept of services, that is independent modules connecting to the same shared infrastructure to process the data.

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

You need Python 3.6+, Apache Spark 2.4+, and docker-compose (latest) installed. Clone the repository:

```bash
git clone https://github.com/astrolabsoftware/fink-broker.git
cd fink-broker
```

Then install the required python dependencies:

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

Finally, define `FINK_HOME` and add the path to the Fink modules in your `.bash_profile`:

```bash
# in ~/.bash_profile
export FINK_HOME=/path/to/fink-broker
export PYTHONPATH=$FINK_HOME/python:$PYTHONPATH
```

Both the [dashboard](user_guide/dashboard.md) and the [simulator](user_guide/simulator.md) rely on docker-compose.


## Getting started

Let's test some functionalities of Fink by simulating a stream of alert, and monitoring it
via the dashboard. Start the dashboard, and go to `http://localhost:5000`:
```bash
./fink start dashboard
# Creating fink-broker_website_1 ... done
# Dashboard served at http://localhost:5000
```

Connect the monitoring service to the stream:
```bash
./fink start monitoring --simulator > live.log &
```

Send a small burst of alerts:
```bash
./fink start simulator
```
Now go to `http://localhost:5000/live.html` and see the alerts coming! The dashboard
should refresh automatically, but do it manually in case it does not work.
Finally stop monitoring and shut down the UI simply using:
```bash
./fink stop monitoring
./fink stop dashboard
```

To get help about `fink`, just type:

```shell
./fink
Monitor Kafka stream received by Apache Spark
Usage:
    to start: ./fink start <service> [-c <conf>] [--simulator]
    to stop : ./fink stop  <service> [-c <conf>]

To get help:
./fink -h or ./fink

Available services are: dashboard, archive, monitoring, classify
Typical configuration would be $FINK_HOME/conf/fink.conf
```
