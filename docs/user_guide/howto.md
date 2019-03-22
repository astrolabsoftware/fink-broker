# Howto

This section gathers Fink's common questions and commands.

## How to get Fink?

Fink is not yet distributed via standard channels such as pip or conda. we plan on doing it soon, but for the moment you need to clone the repo:

```bash
git clone https://github.com/astrolabsoftware/fink-broker.git
cd fink-broker
```

Then install the required python dependencies:

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

Finally, define `FINK_HOME` and add the path to the Fink binaries and modules in your `.bash_profile`:

```bash
# in ~/.bash_profile
export FINK_HOME=/path/to/fink-broker
export PYTHONPATH=$FINK_HOME/python:$PYTHONPATH
export PATH=$FINK_HOME/bin:$PATH
```

## Dashboard

### How to start the dashboard

The dashboard starts with

```bash
fink start dashboard
```

and go to [http://localhost:5000](http://localhost:5000).

### How to change the port for the dashboard?

In the configuration `conf/fink.conf`, change the line:

```bash
FINK_UI_PORT=5000
```

### How to redirect ports to access the dashboard from a cluster?

You might need to redirect the port to get access to the dashboard:

```
# Assuming the port for the dashboard will be FINK_UI_PORT
ssh -L $FINK_UI_PORT:clustername:$FINK_UI_PORT username@clustername
# connection...
fink start dashboard
# Follow the URL (local redirection)
```

## Services

### How do I test the broker?

Just execute the test suite:

```
fink_test
```

You should see plenty of verbose logs from Apache Spark at screen (and yet we have shut most of them!), and finally the coverage.

### How do I start a service?

You can start a service by its name (assuming it is available):

```bash
fink start <service>
```

If you want to know which services are available:

```bash
fink
Monitor Kafka stream received by Apache Spark

 Usage:
 	to start: fink start <service> [-h] [-c <conf>] [--simulator]
 	to stop : fink stop <service> [-h] [-c <conf>]

 To get this help:
 	fink

 To get help for a service:
 	fink start <service> -h

 Available services are: dashboard, archive, monitoring, classify
 Typical configuration would be ${FINK_HOME}/conf/fink.conf
```

### How do I stop a service?

If you launch the service using

```bash
fink start <service>
```

just exit it using CTRL+C. If you put the service in the background, like

```bash
fink start <service> > service.log &
```

You can exit it using:

```bash
fink stop <service>
```

Note that currently this command will stop all services running (not just your service)... This will change soon.

### How do I put my service in the background while keeping the log?

Just redirect the output and escape:

```bash
fink start <service> > service.log &
```

### How do I get help on a service?

You easily get help on a service using:

```bash
fink start <service_name> -h
```

This will also tells you which configuration parameters are used, e.g.

```bash
fink start monitoring -h
usage: monitor_fromstream.py [-h] servers topic finkwebpath

Monitor Kafka stream received by Spark

positional arguments:
  servers      Hostname or IP and port of Kafka broker producing stream.
               [KAFKA_IPPORT]
  topic        Name of Kafka topic stream to read from. [KAFKA_TOPIC]
  finkwebpath  Folder to store UI data for display. [FINK_UI_PATH]

optional arguments:
  -h, --help   show this help message and exit
```

### Spark's verbosity is overwhelming my logs!

Yes... By default Apache Spark outputs many many things - good for debug, but awful in production. In Fink we provide a utils to deal with Spark logs in your service:

```python
# inside your service
from fink_broker.sparkUtils import quiet_logs

# Your application
# ...

# Grab the running Spark Session,
# otherwise create it.
spark = SparkSession \
    .builder \
    .appName("classifyStream") \
    .getOrCreate()

# Set logs to be quieter (Just error messages)
# Put WARN or INFO for debugging, but you will have to dive into
# a sea of millions irrelevant messages for what you typically need...
quiet_logs(spark.sparkContext, log_level="ERROR")

# The rest of your application
# ...
```

### How can I test my service before production?

You can use it with the [simulator](simulator.md). See also the section [Testing Fink](testing-fink.md).

## Configuration

### How can I use a custom configuration?

Take inspiration from `conf/fink.conf`, and link your service to your custom configuration using

```bash
fink start <service> -c /path/my/custom_conf.conf
```

we recommend to not edit directly `conf/fink.conf`, but rather edit a copy of it.

## Apache Kafka

### How do I connect to a secured Kafka cluster?

First, you need to create a file (let's call it `jaas.conf`) with your username and password (assuming PLAINTEXT):

```java
// In jaas.conf
KafkaClient {
      org.apache.kafka.common.security.plain.PlainLoginModule required
       username="your_user_name_or_api_key"
        password="your_pwd";
 };
```

Then in the configuration file (assuming the `jaas.conf` is in the current directory), adds:

```bash
# in conf/myconf.conf
EXTRA_SPARK_CONFIG='<your config> --files jaas.conf
--driver-java-options "-Djava.security.auth.login.config=./jaas.conf"
--conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./jaas.conf"'
```

You will find such a file in the folder `${FINK_HOME}/conf`

## Troubleshooting

### Failed to construct kafka consumer

This usually means there is no Kafka producer running at the `KAFKA_IPPORT` you specified in your [configuration](configuration.md). Check if there is no typo.

### The simulator is publishing alerts, but I do not see anything on the service side.

Make sure you use correctly the same `KAFKA_IPPORT_SIM` and `KAFKA_TOPIC_SIM` in both sides (simulator and service).


### fink: line 91: /conf/fink.conf: No such file or directory

You forgot to set `FINK_HOME`. The best is to add the path to Fink in your `.bash_profile`:

```bash
# in ~/.bash_profile
export FINK_HOME=/path/to/fink-broker
export PYTHONPATH=$FINK_HOME/python:$PYTHONPATH
```
