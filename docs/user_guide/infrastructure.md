# Infrastructure

Fink decouples resources needed for listening to the stream (online, critical), and resources used for services: scalable, robust, and modular!

![Screenshot](../img/infrastructure.svg)

## Spark Structured streaming

Fink is principally based on the recent [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) module introduced in Spark 2.0 (see [paper](https://cs.stanford.edu/~matei/papers/2018/sigmod_structured_streaming.pdf)), and especially its integration with Apache Kafka (see [here](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)). Structured streaming is a stream processing engine built on the Spark SQL engine, hence it combines the best of the two worlds.
The idea behind it is to process data streams as a series of small batch jobs, called micro-batch processing. As anything in Spark, it provides fast, scalable, fault-tolerant processing, plus end-to-end exactly-once stream processing.

## Datastore

The most critical part in a context of big data is to capture as fast as possible the stream, and store information efficiently and reliably. We start with one Spark Structured Streaming job reading and decoding Avro events sent from telescopes, and writing them to partitioned Parquet tables in distributed file systems such as HDFS (Raw database). Then multi-modal analytics take place and several other batch and streaming jobs query this table to process further the data, and push relevant alert data into an HBase table (Science database).

This main service is described in the [database](database.md) section.

## Services

### Services & dashboards

Fink provides built-in services, described in [Available Services](available-services.md). They operate at different timescales, and with various objectives:

- Operating from the stream or from the database
- Real time or post-processing of alerts.
- Urgent decision to take (observation plan).

Each service is Spark job on the database - either raw or science. All services are linked to the [dashboard](dashboard.md), and you can easily follow live and interactively the outputs. Note you can easily define your own service in Fink (i.e. your favourite ML code!), and connect it to the alert database. See [Adding a new service](adding-new-service.md) for more information.

### AstroLabNet

[AstroLabNet](https://hrivnac.web.cern.ch/hrivnac/Activities/Packages/AstroLabNet/) is a front-end to ease the manipulation of the science database (HBase). It allows to

* Access distributed data.
* Deploy jobs to data.
* Move data between servers.
* Arrange data streaming and updating.

Note that the development of AstroLabNet is done outside of Fink. Resources: [code source](https://github.com/hrivnac/AstroLabNet), [documentation](https://hrivnac.web.cern.ch/hrivnac/Activities/Packages/AstroLabNet/).

## Simulating alert data

In Fink, we want also to test our services before deploying them full-scale. We provide a simple stream simulator based on a dockerized Kafka & Zookeeper cluster:

```bash
fink start simulator
```

This will set up the simulator and send a stream of alerts. Then test a service in simulation mode by specifying `--simulator`:

```bash
fink start <service> --simulator
```

See [Simulator](simulator.md) for more information.

## Redistributing Alerts

Part of the incoming stream will be also redirected outside for other brokers and individual clients. See [Redistributing Alerts](streaming-out.md) for more information.
