# Redistributing Alerts

One goal of the broker is to redistribute alert packets to the community. Therefore we are developing a service for streaming out alerts from the database to the world.

## Components

The two major components of Fink's distribution system are:

1. Kafka Cluster
2. Spark Process (acts as the Producer)

## Kafka Cluster
The Kafka Cluster consists of one or more Kafka broker(s) and a Zookeeper server that monitors and manages the Kafka brokers.
It is important to note that the Kafka Cluster for Redistribution of alerts is different from the one used in [simulator](simulator.md).
<br>
You will need [Apache Kafka](https://kafka.apache.org/) 2.2+ installed. Define `KAFKA_HOME` as per your installation in your ~/.bash_profile.

```bash
# in ~/.bash_profile
# as per your Kafka installation directory
export KAFKA_HOME=/usr/local/kafka
```
<br>
We provide a script `fink_kafka` to efficiently manage the Kafka Cluster for Redistribution. The help message shows the available
services:

```bash
Manage Finks Kafka Server for Alert Distribution
 USAGE:

 	 start
 		 Starts a Zookeeper and a Kafka Server

 	 stop
 		 Stops a running Kafka and Zookeeper Server

 	 -h, --help
 		 To view this help message

 	 --create-topic <TOPIC>
 		 Creates a topic named <TOPIC> if it does not already exist
```

## Spark process
The Spark process is responsible for reading the alert data from the [Science Database](database.md#science-database-structure),
converting the data into avro packets and publishing them to Kafka topic(s).
<br>
To test the working of the distribution system
<br>
First start the Fink's Kafka Cluster and create a test topic:

```bash
# Start the Kafka Cluster
fink_kafka start
# Create a test topic
fink_kafka --create-topic distribution_test
```

This will start the Kafka Cluster and create a topic "distribution_test" if it didn't already exist.
Now start the distribution service:

```bash
# Start the distribution service
fink start distribution
```
Running the distribution service results in the following:

Firstly, the Science Database is read.
Then the alert data is converted into avro and the schema is stored at the path given in configuration:

```bash
DISTRIBUTION_SCHEMA=${FINK_HOME}/schemas/distribution_schema
```

This schema can be used by a consumer service to read the Kafka messages. To learn more about configuring Fink see [configuration](configuration.md). Thirdly, the avro packets are produced as Kafka messages on the running Kafka Cluster.
<br><br>
To check the working of the distribution pipelline we provide a Spark Consumer that reads the messages published
on the topic "distribution_test" and uses the schema above to convert them back to Spark DataFrame. This can be run using:

```bash
fink start distribution_test
```
