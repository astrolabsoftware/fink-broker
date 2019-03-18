# Fink simulator

For testing purposes, Fink includes a service to simulate incoming streams. The service is provided through docker-compose, and include Kafka and Zookeeper clusters. By default alerts will be published on the port 29092 (localhost), but you can change it in the [configuration](configuration.md). To start the simulator and send alerts, just execute:

```bash
./fink start simulator
```

With the default configuration, it will publish 100 alerts during 10 seconds in a topic called `ztf-stream-sim`. You can change all of it in the configuration file. All services can be run on the simulated stream by just specifying the argument `--simulator`:

```bash
./fink start <service> --simulator
```

For example for demo purposing, you can easily start the monitoring service on the simulated stream with:

```bash
./fink start monitoring --simulator > live_sim.log &
```

and watch the stream at [http://localhost:5000/live.html](http://localhost:5000/live.html) (change the port with the one in your configuration).
