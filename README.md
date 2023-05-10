# <img src=".github/Fink_PrimaryLogo_WEB.png" width=150 />

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=finkbroker&metric=alert_status)](https://sonarcloud.io/dashboard?id=finkbroker)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=finkbroker&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=finkbroker)
[![Sentinel](https://github.com/astrolabsoftware/fink-broker/actions/workflows/test.yml/badge.svg)](https://github.com/astrolabsoftware/fink-broker/actions/workflows/test.yml)
[![PEP8](https://github.com/astrolabsoftware/fink-broker/workflows/PEP8/badge.svg)](https://github.com/astrolabsoftware/fink-broker/actions?query=workflow%3APEP8)
[![codecov](https://codecov.io/gh/astrolabsoftware/fink-broker/branch/master/graph/badge.svg)](https://codecov.io/gh/astrolabsoftware/fink-broker)
[![Documentation Status](https://readthedocs.org/projects/fink-broker/badge/?version=latest)](https://fink-broker.readthedocs.io/en/latest/?badge=latest)

## What is Fink?

Fink is an alert broker, that is a layer between astronomical alert issuers and the scientific community analysing the alert data. It exposes services to help the scientists to efficiently analyse the alert data from telescopes and surveys. Among several, it collects and stores alert data, enriches them with information from other surveys and catalogues or user-defined added values such as machine-learning classification scores, and redistributes the most promising events for further analyses, including follow-up observations.


Fink's main scientific objective is to optimize the scientific impact of the [Rubin Observatory](https://www.lsst.org/) alert data stream. We do not limit ourselves to a specific area, but instead our ambition is to study the transient and variable sky as a whole, from Solar system objects to galactic and extragalactic science. In practice thanks to the [Zwicky Transient Facility](https://www.ztf.caltech.edu/) alert stream, we are active since 2019 on Solar system objects, young stellar objects, microlensing, supernovae, kilonovae, gamma-ray bursts, active galactic nuclei, and even anomaly detection. On the technological side, Fink aims at providing a robust infrastructure and state-of-the-art streaming services to Rubin scientists, to seamlessly enable user-defined science cases in a big data context.

## How Fink works?

The current Fink platform works in four steps. First, alerts from multiple streams are continuously ingested and stored on disk (Apache Spark Structured Streaming). Second, alerts satisfying the quality cuts defined by the broker team are processed by a set of science modules. These science modules -- currently a dozen, spanning solar system objects to galactic and extra-galactic science -- are independent processing units developed by the community of users, and deployed in the Fink platform. They can work on a single input alert stream, or combine several streams together. These science modules enrich the initial alert packets using several techniques such as cross-match with external catalogs of astronomical objects, or classification using machine or deep learning based algorithms. All added-values are made public for the benefit of everyone. Third, enriched alert packets are filtered based on their content, and the most promising events are redistributed to the scientific community in real-time (Apache Kafka). The filtering is again community-driven, and users design and deploy filters to receive tailored information in real-time. Finally, all enriched alert packets are stored in a database (Apache HBase) for permanent access and for further analyses.

## The Fink galaxy

Fink is made of several blocks that interconnect to provide all services:

- [Fink Broker](https://github.com/astrolabsoftware/fink-broker): Astronomy Broker based on Apache Spark.
- [Fink Science Modules](https://github.com/astrolabsoftware/fink-science): Define your science modules to add values to Fink alerts.
- [Fink Filters](https://github.com/astrolabsoftware/fink-filters): Define your filters to create your alert stream in Fink.
- [Fink Science Portal](https://github.com/astrolabsoftware/fink-science-portal): Web application and REST API to access all alert data.
- [Fink Client](https://github.com/astrolabsoftware/fink-client): Light-weight client to manipulate alerts sent from Kafka.
- [Fink Utils](https://github.com/astrolabsoftware/fink-utils): Various utilities used across different repositories in Fink.
- [Fink Alert Simulator](https://github.com/astrolabsoftware/fink-alert-simulator): Simulate alert streams for the Fink broker.

All are open source, and we thank all contributors!

## Useful links

- Website: https://fink-broker.org
- Documentation website: https://fink-broker.readthedocs.io
- Science Portal: https://fink-portal.org
- Publications: https://fink-broker.org/papers
- Release notes: https://fink-broker.readthedocs.io/en/latest/release-notes

## Outside academia

Fink has participated to a number of events with the private sector, in particular:

* 05/20: Fink has been selected to the Google Summer of Code 2020 (CERN-HSF org)! Congratulations to [saucam](https://github.com/saucam) who will work on the project this year. _Focus on graph database with JanusGraph_
* 10/19: Fink was featured at the [Spark+AI Summit Europe 2019](https://www.databricks.com/session_eu19/accelerating-astronomical-discoveries-with-apache-spark).
* 05/19: Fink has been selected to the Google Summer of Code 2019 (CERN-HSF org)! Congratulations to [abhishekchauhn](https://github.com/abhishekchauhn) who will work on the project this year. _Focus on alert redistribution with Apache Kafka_
