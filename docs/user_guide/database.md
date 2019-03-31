# Database

## Why archiving first?

If we had all our jobs reading from the upstream Kafka cluster, we would consume too much resources, and place high load on Kafka. Hence the first service of Fink is to archive the incoming streams, as fast as possible. We start with one Spark Structured Streaming job reading and decoding Avro events from telescopes, and writing them to partitioned Parquet tables in distributed file systems such as HDFS. Then multi-modal analytics take place and several other batch and streaming jobs query this table to process further the data, and other reports via interactive jobs, redirecting outputs to the dashboard.

![Screenshot](../img/archiving.png)

## Database structure

We currently operates the conversion from Avro (alerts) to Parquet (database) for mainly two reasons:

- Parquet is a built-in output sinks in Structured Streaming, not Avro. We can use a custom sink for Avro, but as Parquet is better integrated with the Spark ecosystem at this point, we stick to it for the moment.
- Other services (post-processing) integrates better with Parquet for the moment. Only the streaming out part would need re-conversion to Avro (Kafka style).

The archiving part is crucial, and must pass a number of stress tests (not exhaustive!):

- What happens if we lose some machines hosting the broker?
- What happens if there is a broker shutdown?
- What happens if there are sudden bursts of alerts?
- What happens if LSST starts to send alerts at a crazy sustained rate?

Extensive benchmarks and resources sizing are under study. The main Parquet database is stored in HDFS (fault-tolerant), and data are partitioned hourly by topic name (`topic/YYYY/MM/dd/hh`). To launch the archiving service, just use:

```bash
fink start archive > archiving.log &
```

Just make sure you attached the `archive` service to disks with large enough space! After the first alerts come, you will have something like:

```bash
# in $FINK_ALERT_PATH
_spark_metadata\
topic=ztf-stream-sim\
  year=2019\
    month=03\
      day=25\
        hour=09\
          part-00000-de57de35-7a77-49f3-af89-d7cbb4a6cd0c.c000.snappy.parquet
          ...
```


Note we perform a data compression (snappy). The compression factor will depend on the triggering time and incoming packet size, but is typically a factor of 1.5. To define the archiving location, see `conf/fink.conf`, or follow steps in [Configuration](configuration.md).

## Monitoring the data transfer

There is a monitoring service attached to the database construction. Unfortunately at the time of writing, there is no built-in listeners in pyspark (2.4) to monitor structured streaming queries. So we had to develop custom tools, and redirect information in the Fink [dashboard](dashboard.md). This is automatically done when you start the `archive` service. Just launch the Fink dashboard and go to `http://localhost:5000/live.html` to see the incoming rate and consumption (archiving) rate:

```bash
fink start dashboard
```

You can stop the archiving at anytime using:

```bash
fink stop archive
```

Note this will stop all Fink services running.
