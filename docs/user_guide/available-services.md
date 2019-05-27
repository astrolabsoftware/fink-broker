# Available Fink services

<script src="https://code.jquery.com/jquery-3.1.1.min.js"></script>
<script src="https://code.highcharts.com/stock/highstock.js"></script>
<script src="https://code.highcharts.com/stock/modules/exporting.js"></script>
<script src="https://code.highcharts.com/stock/modules/export-data.js"></script>
<script src="https://code.highcharts.com/stock/modules/data.js"></script>

In addition to [archiving](database.md) the streams, Fink provides built-in services operating at different levels:

- Operating from the stream or from the database
- Real time or post-processing of alerts.
- Urgent decision to take (observation plan).

and you can connect your favourite code to the database, getting data access within seconds of publication (see below)!

![Screenshot](../img/monitoring.png)

Given the crazy rate of alerts, it seems insane to live monitor each alert individually, and on short timescales it makes more sense to focus on some physically motivated statistics on the stream, target potential outliers, and highlight problems. On longer timescales, we want of course also to be able to access, inspect, and process each alert received by Fink. Each service is typically a Spark job on the database (to avoid putting too high load on the alert sender), but the `stream2raw` service that operates directly on the stream.

Each Spark job is either batch or streaming, or both (multi-modal analytics). All services are linked to the [dashboard](dashboard.md), and you can easily follow live and interactively the outputs. To start a service, just execute:

```bash
fink start <service_name> > output.log &
```

For convenience, we redirect the console output to a log file, and escape. You can also use tools like `nohup`. You easily get help on a service using:

```bash
fink start <service_name> -h
```

This will also tells you which configuration parameters are used. To stop services, simply use:

```bash
fink stop services
```

Note this will stop all Fink services running (this will change in the future to stop services individually). You can easily define your own service in Fink, and connect it to the alert database.

## Monitoring

The monitoring of the stream is typically done with the archiving service. But for test reasons, we sometimes also want to monitor the stream directly. The service will give you information on the incoming rate (number of alerts per second from Kafka), the processing rate (number of alerts per second received and processed by Spark), and the batch time (time to process a micro-batch of alerts).

To use this service, just execute:

```bash
fink start checkstream > live.log &
```
Again, this will query the stream directly - so be careful!

<div id="container_live"></div>
<script src="https://fink-broker.readthedocs.io/en/latest/js/live.js"></script>

Note that you also browse history data at any point, using the History tab:

<div id="container_hist"></div>
<script src="https://fink-broker.readthedocs.io/en/latest/js/hist.js"></script>

## Early classification

This service operates from the database and connects to external database such as the CDS Strasbourg, and perform real-time classification for known objects based on incoming alert positions on sky. The idea is to classify the alerts into 2 big classes: known objects vs unknown objects. And for known objects, we want to retrieve known information such as the name and type of the object.

It is a standard problem of cross-match, but in real-time! In order to not rely on hard-coded catalogs, Fink connects to remote databases, and query them with the data in our alerts (ra/dec-based). Fink uses the CDS [X-Match](http://cdsxmatch.u-strasbg.fr/) service, and the SIMBAD bibliographical database (updated every day). It requires a bit of tweaking so avoid overcrowding the CDS servers with too many requests. The idea is to send a list of alerts at once. The list is generated inside each micro-batch.

The early classification is part of the `raw2science` service:

```bash
fink start raw2science > raw2science.log &
```

Then to retrieve the type of alert, open a pyspark shell for example:

```bash
# Start HBase service
/path/to/hbase/start-hbase.sh

# Launch a pyspark shell with fink dependencies loaded
source conf/fink.conf
PYSPARK_DRIVER_PYTHON=ipython pyspark --jars $FINK_JARS --packages $FINK_PACKAGES
```

and type:

```python
from fink_broker.sparkUtils import init_sparksession
import json

# Grab the running Spark Session,
# otherwise create it.
spark = init_sparksession(
  name="readingScienceDB", shuffle_partitions=2, log_level="ERROR")

# File containing the HBase catalog - it is generated when you
# run the raw2science service.
with open('catalog.json') as f:
    catalog = json.load(f)

catalog_dic = json.loads(catalog)

df = spark.read.option("catalog", catalog)\
  .format("org.apache.spark.sql.execution.datasources.hbase")\
  .load()

print("Number of entries in {}: ".format(
  catalog_dic["table"]["name"]), df.count())

df_of_types = df.groupBy("simbadType").count()

# Display the number of entries per astronomical objects found:
df_of_types.show()
```

<!-- and go to `http://localhost:5000/classification.html`

<div id="container_bar"></div>
<script src="https://fink-broker.readthedocs.io/en/latest/js/bar.js"></script> -->

For description of types, see [here](http://cds.u-strasbg.fr/cgi-bin/Otype?X).

## Alert tracker (WIP)

Display on full sky map alert informations. Use Aladin. WIP.

## Your code goes here!

Thanks to Fink modularity you can easily come with your ideas and codes, and connect to Fink database! You can either run your code live, or in batch, or interactively (via Jupyter notebook). If you would like to use Fink for your analysis, feel free to contact us with your science proposal, we would be happy to help in the integration!
