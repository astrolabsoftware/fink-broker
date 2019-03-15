# Infrastructure

Fink exposes two main bricks: a robust core infrastructure, and several services.

![Screenshot](/platform_wo_logo_hor.png)

## Spark Structured streaming

Fink is principally based on the recent [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) development introduced in Spark 2.0 (see [paper](https://cs.stanford.edu/~matei/papers/2018/sigmod_structured_streaming.pdf)), and especially its integration with Apache Kafka (see [here](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)). Structured streaming is a stream processing engine built on the Spark SQL engine, hence it combines the best of the two worlds.
The idea behind it is to process data streams as a series of small batch jobs, called micro-batch processing. As anything in Spark, it provides fast, scalable, fault-tolerant processing, plus end-to-end exactly-once stream processing.

## Database

If we had all our jobs reading from the upstream Kafka cluster, we would consume too much resources, and place high load on it. Hence the first service of Fink is to archive the incoming streams, as fast as possible. We start with one Spark Structured Streaming job reading and decoding Avro events from telescopes, and writing them to partitioned Parquet tables in distributed file systems such as HDFS. Then multi-modal analytics take place and several other batch and streaming jobs query this table to process further the data, and other reports via interactive jobs, redirecting outputs to the webUI.

![Screenshot](/archiving.png)

We currently operates the conversion from Avro to Parquet for two reasons:

- Parquet is a built-in output sinks in Structured Streaming, not Avro. We can use a custom sink for Avro, but as Parquet is better integrated with the Spark ecosystem at this point, we stick to it for the moment.
- Other services (post-processing) integrates better with Parquet for the moment. Only the streaming out part would need re-conversion to Avro of the data.

The archiving part is crucial, and must respect a number of criteria:

- The archiving must be done as quickly as possible.
- The archiving must resist to bursts of alerts.
- In case of several days of shut down, the archiving must be able to archive late data while ingesting new data.
- The database must be fault-tolerant, and must allow fast concurrent access.

Concerning the first 3 points, benchmarks and resources sizing are under work. For the last point, our Parquet database is stored in HDFS, and data are partitioned hourly (`YYYY/MM/dd/hh`). To launch the archiving service, just use:

```bash
./fink start archive > archiving.log &
```

Just make sure you attached the `archive` service to disks with large enough space! To define the location, see `conf/fink.conf`, or follow steps in [Configuration](/user_guide/configuration/).

There is a monitoring service attached to the database construction. Unfortunately at the time of writing, there is no built-in listeners in pyspark to monitor structured streaming queries. So we had to develop custom tools, and redirect information in the Fink [webUI](/user_guide/webui/). This is automatically done when you start the `archive` service. Just launch the Fink UI and go to `http://localhost:5000/live.html` to see the incoming rate and consumption (archiving) rate:

```bash
./fink start ui
```

You can stop the archiving at anytime using:

```bash
./fink stop archive
```

Note this will stop all Fink services running.

## Services

### Services & dashboards

![Screenshot](/monitoring.png)

Fink must operate at different timescales. But all timescales must be treated differently, and by different services. Given the crazy rate of alerts, it seems insane to live monitor each alert individually. So on short timescale, it makes more sense to focus on some physically motivated statistics on the stream, target potential outliers, and highlight problems. On longer timescales, we want of course also to be able to access, inspect, and process each alert received by Fink.

We provide some built-in services in Fink, operating at different levels:

- Early classification: Perform the cross-match between incoming alert position and external catalogs to start classifying the object type. Short timescale.
- Outlier detection: WIP. Short timescale.
- Light-curve inspection: WIP. Long timescale.

Each service is Spark job on the database - either batch or streaming, or both (multi-modal analytics). All services are linked to the [webUI](/user_guide/webui/), and you can easily follow live and interactively the outputs. For example, if you want to start classifying the alerts, just launch:

```bash
./fink start classify > classify.log &
```

and go to `http://localhost:5000/classification.html`


Note you can easily define your own service in Fink, and connect it to the alert database. See [Adding a new service](/user_guide/adding-new-service/) for more information.

### AstroLabNet

WIP.

### Streaming out

![Screenshot](/streaming.png)

WIP

## Infrastructure for simulation

In Fink, we want also to test our services before deploying them full-scale. We provide a simple stream simulator based on a dockerized Kafka & Zookeeper cluster:

```bash
./fink start simulator
```

This will set up the simulator and send a stream of alerts. Then test a service in simulation mode by specifying `--simulator`:

```bash
./fink start <service> --simulator
```

See [Simulator](/user_guide/simulator/) for more information.
