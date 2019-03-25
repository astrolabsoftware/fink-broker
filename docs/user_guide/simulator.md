# Fink simulator

For testing purposes, Fink includes a service to simulate incoming streams. With this stream simulator part, you can easily develop applications locally and deploy at scale only when you are ready!

The service is provided through docker-compose, and include Kafka and Zookeeper clusters. The simulator generates the stream from alerts stored on disk. We provide a script to download a subset of ZTF alerts that will be used to generate streams:

```bash
cd ${FINK_HOME}/datasim
./download_ztf_alert_data.sh
# ...download 499 alerts
```

You can change this folder with your data, but make sure you correctly set the schema and the data path in the configuration:

```bash
# in configuration file
FINK_ALERT_SCHEMA=...
FINK_DATA_SIM=...
```

By default alerts will be published on the port 29092 (localhost), but you can change it in the [configuration](configuration.md). To start the simulator and send alerts, just execute:

```bash
fink start simulator
```

With the default configuration, it will publish 100 alerts during 10 seconds in a topic called `ztf-stream-sim`. You can change all of it in the configuration file. Note if you want to publish more alerts than what is available on disk, alerts will be replicated on-the-fly to match the desired number. All services can be run on the simulated stream by just specifying the argument `--simulator`:

```bash
fink start <service> --simulator
```

For example for demo purposing, you can easily start the monitoring service on the simulated stream with:

```bash
fink start monitor --simulator > live_sim.log &
```

and watch the stream at [http://localhost:5000/live.html](http://localhost:5000/live.html) (change the port with the one in your configuration file).
