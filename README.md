[![Build Status](https://travis-ci.org/astrolabsoftware/fink-broker.svg?branch=master)](https://travis-ci.org/astrolabsoftware/fink-broker)
[![codecov](https://codecov.io/gh/astrolabsoftware/fink-broker/branch/master/graph/badge.svg)](https://codecov.io/gh/astrolabsoftware/fink-broker)

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=finkbroker&metric=alert_status)](https://sonarcloud.io/dashboard?id=finkbroker): [![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=finkbroker&metric=code_smells)](https://sonarcloud.io/dashboard?id=finkbroker) [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=finkbroker&metric=coverage)](https://sonarcloud.io/dashboard?id=finkbroker) [![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=finkbroker&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=finkbroker)

# <p align="left"><img width="150" src="docs/fink_circle.png"/></p>

Fink is a broker infrastructure using Apache Spark Streaming to receive and process alerts issued from telescopes all over the world.

## Rationale

The design of Fink is driven by three pillars:

* **Simplicity** the broker should be simple enough to be used by a majority of scientists and maintained in real-time. This means the exposed API must be easily understood by anyone, and the code base should be as small as possible to allow easy maintenance and upgrade.
* **Scalability** the broker's behaviour should be the same regardless the amount of incoming data. This implies the technology used for this is scalable.
* **Flexibility** the broker structure should allow for easy extension. As data will come, new features will be added, and the broker should be able to incorporate those smoothly. In addition, the broker should be able to connect to a large numbers of external tools and frameworks to maximize its scientific production without redeveloping tools.

Fink core is based on the [Apache Spark](http://spark.apache.org/) framework, and more specifically its [streaming module](http://spark.apache.org/streaming/). The language chosen for the API is Python, which is widely used in the astronomy community, has a large scientific ecosystem and easily connects with existing tools.

## Installation

You need Python 3.5+, and Apache Spark 2.4 installed.

Having `docker-compose` is a plus for the webUI (python `http.server` used instead).

add `FINK_HOME=...` in your `~/.bash_profile`.

### How to use Fink?

### Structure

Fink exposes two main bricks: a robust core infrastructure, and several services.

<p align="center"><img width="600" src="docs/platform_wo_logo_hor.png"/></p>

*describe me*

#### Workflow

*How to play with fink*

```bash
# Launch the UI
# Go to localhost:5000/index.html
./fink start ui

# Decode and archive the stream data
# Monitor it at localhost:5000/live.html
./fink start archive &> arch.log &

# Start also some filtering and aggregation on the stream
# Relax at localhost:5000/aggregation.html
./fink start aggregation &> agg.log &

# Stop the all the services
./fink stop services

# Stop the ui
./fink stop ui
```

#### Configuration

*How to configure fink*

#### Secured connection

*How to connect Spark to protected Kafka*

### Extension

*How to make your own aggregation filter*

### Deployment

*Use Apache Spark cluster and launcher scripts.*
