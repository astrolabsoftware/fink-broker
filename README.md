[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=finkbroker&metric=alert_status)](https://sonarcloud.io/dashboard?id=finkbroker) [![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=finkbroker&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=finkbroker)
[![Build Status](https://travis-ci.org/astrolabsoftware/fink-broker.svg?branch=master)](https://travis-ci.org/astrolabsoftware/fink-broker)
[![codecov](https://codecov.io/gh/astrolabsoftware/fink-broker/branch/master/graph/badge.svg)](https://codecov.io/gh/astrolabsoftware/fink-broker) [![Documentation Status](https://readthedocs.org/projects/fink-broker/badge/?version=latest)](https://fink-broker.readthedocs.io/en/latest/?badge=latest)

# Latest News

* Fink is participating to the LSST [call](https://ldm-682.lsst.io/) for community broker! Stay tuned for more.
* Fink has been selected to the Google Summer of Code 2019! Congratulation to [cAbhi15](https://github.com/cAbhi15) who will work on the project this year!

# Fink

Fink is a broker infrastructure enabling a wide range of applications and services to connect to large streams of alerts issued from telescopes all over the world.

## Rationale

The design of Fink is driven by three pillars:

* **Simplicity** the broker should be simple enough to be used by a majority of scientists and maintained in real-time. This means the exposed API must be easily understood by anyone, and the code base should be as small as possible to allow easy maintenance and upgrade.
* **Scalability** the broker's behaviour should be the same regardless the amount of incoming data. This implies the technology used for this is scalable.
* **Flexibility** the broker structure should allow for easy extension. As data will come, new features will be added, and the broker should be able to incorporate those smoothly. In addition, the broker should be able to connect to a large numbers of external tools and frameworks to maximize its scientific production without redeveloping tools.

Fink core is based on the [Apache Spark](http://spark.apache.org/) framework, and more specifically it uses the [Structured Streaming processing engine](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html). The language chosen for the API is Python, which is widely used in the astronomy community, has a large scientific ecosystem and easily connects with existing tools.

## Documentation

Fink's documentation is hosted [here](https://fink-broker.rtfd.io).
