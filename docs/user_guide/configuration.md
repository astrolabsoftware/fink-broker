# Configuration

## Configuring Fink

Fink's configuration is done via a configuration file typically stored in `conf/`.
There are 3 main categories:

- Infrastructure (Kafka & Spark)
- Dashboard
- Simulator

The configuration file is called each time you call fink. By default, fink takes the one under `conf/fink.conf`, but you can also specify it manually:

```bash
fink start <service> -c /path/to/myconf.conf
```

## Configuring Apache Kafka & Apache Spark

### Apache Kafka

First you have to provide the IP and the port of the Kafka cluster publishing streams (make sure the Kafka cluster is running):
```
# Kafka producer stream location
KAFKA_IPPORT="xx.yy.zz.ww:port"
```

Then provide the name of the topic:
```
KAFKA_TOPIC="mytopic"
```
Note it can be `topic1,topic2,etc`, or a pattern `topic.*`.

You can specify from which offset you want to start pulling data. Options are:
latest (only new data), earliest (connect from the oldest
offset available), or a json string (see [here](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html) for detailed information).
```
KAFKA_STARTING_OFFSET="latest"
```

Finally if you need to connect to a secured Kafka cluster, enter your credentials in the `jaas.conf` file, and pass them using:
```
# Should be Kafka secured options actually (to allow connection to Kafka)
SECURED_KAFKA_CONFIG='--files jaas.conf --driver-java-options
"-Djava.security.auth.login.config=./jaas.conf" --conf
"spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./jaas.conf"'
```
### Apache Spark

Assuming you have installed Apache Spark on a cluster, you need to specify the running mode:
```
# Apache Spark mode
SPARK_MASTER="local[*]" # or yarn, or spark://ip_driver:port
```

You can also any Spark options (memory requirement, number of executors, etc.):
```
# Should be Spark options actually (to allow cluster resources!)
EXTRA_SPARK_CONFIG=""
```

Note that while Apache Avro is supported natively since Spark 2.4, you still need to include the external library at runtime. Same for the Kafka integration. We provide the maven coordinates - just make sure it corresponds to your Spark version (must be 2.4+):
```
# These are the Maven Coordinates of dependencies for Fink
# Change the version according to your Spark version.
FINK_PACKAGES=\
org.apache.spark:spark-streaming-kafka-0-10-assembly_2.11:2.4.1,\
org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1,\
org.apache.spark:spark-avro_2.11:2.4.1
```

In addition to packages, you can specify external libraries as jars:
```
# These two are shipped with the code - use the latest version
FINK_JARS=${FINK_HOME}/libs/fink-broker_2.11-0.2.0.jar,\
${FINK_HOME}/libs/shc-core-1.1.3-2.4-s_2.11.jar
```

Note that part of Fink is written in Scala, and you need to specify the jars (under `libs/`). If you make change to the code, do not forget to re-compile the source.

As described in [Infrastructure](infrastructure.md), data streams are processed as a series of small batch jobs. You can specify the time interval between two triggers (second), i.e. the timing of streaming data processing. If `0`, the query will be executed in micro-batch mode, where micro-batches will be generated as soon as the previous micro-batch has completed processing. Note that this timing is also used for updating data for the dashboard.
```
FINK_TRIGGER_UPDATE=2
```

You also need to provide the schema to decode the alert. To make it simple,
Fink takes one alert as the reference:
```bash
FINK_ALERT_SCHEMA=${FINK_HOME}/schemas/template_schema_ZTF.avro
```

Finally, you need to provide location on disk to save the incoming alerts and checkpoints.
They can be in local FS (`files:///path/`) or in distributed FS (e.g. `hdfs:///path/`). Be careful though to have enough disk space!
```
FINK_ALERT_PATH=${FINK_HOME}/archive/alerts_store
FINK_ALERT_CHECKPOINT_RAW=${FINK_HOME}/archive/alerts_raw_checkpoint
FINK_ALERT_CHECKPOINT_SCI=${FINK_HOME}/archive/alerts_sci_checkpoint
```

Do not forget to give a name to your HBase science table:
```
# The name of the HBase table
SCIENCE_DB_NAME="test_catalog"
```

And link to the HBase XML configuration file of your cluster. The file must be
located in the `conf` folder of the Spark installation:

```
# HBase configuration file - must be under ${SPARK_HOME}/conf
# You can find an example in ${FINK_HOME}/conf
HBASE_XML_CONF=${SPARK_HOME}/conf/hbase-site.xml
```

## Configuring the dashboard

Where the web data will be posted and retrieved by the dashboard.
For small files, you can keep this location. If you plan on having large files, change to a better suited location.
```
FINK_UI_PATH=${FINK_HOME}/web/data
```

Port to access the dashboard:
```
FINK_UI_PORT=5000
```

## Configuring the Simulator

The idea is to simulate a fake stream via Kafka inside docker, and access it locally from outside docker. You can set the port and the name of the topic:
```
KAFKA_PORT_SIM=29092
KAFKA_IPPORT_SIM="localhost:${KAFKA_PORT_SIM}"
KAFKA_TOPIC_SIM="ztf-stream-sim"
```

The simulator generates the stream from alerts stored on disk. You need to
provide the folder containing such alerts:
```
FINK_DATA_SIM=${FINK_HOME}/datasim
```

The time in between two alerts:
```
# Time between 2 alerts (second)
TIME_INTERVAL=0.1
```

The total number of alerts to send:
```
# Total number of alerts to send
POOLSIZE=100
```
