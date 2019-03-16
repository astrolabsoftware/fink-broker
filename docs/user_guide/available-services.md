# Available Fink services

Fink provides built-in services operating at different levels:

- Operating from the stream or from the database
- Real time or post-processing of alerts.
- Urgent decision to take (observation plan).

Given the crazy rate of alerts, it seems insane to live monitor each alert individually, and on short timescales it makes more sense to focus on some physically motivated statistics on the stream, target potential outliers, and highlight problems. On longer timescales, we want of course also to be able to access, inspect, and process each alert received by Fink. Each service is a Spark job on the database, but the archive service that operates directly on the stream.

Each Spark job is either batch or streaming, or both (multi-modal analytics). All services are linked to the [dashboard](dashboard.md), and you can easily follow live and interactively the outputs. For example, if you want to start classifying the alerts from the database, just launch:

```bash
./fink start classify > classify.log &
```

and go to `http://localhost:5000/classification.html`

Note you can easily define your own service in Fink, and connect it to the alert database. See [Adding a new service](adding-new-service.md) for more information.

## Archive (from stream)

## Monitoring (from stream)

## Early classification (from database)

Perform the cross-match between incoming alert position and external catalogs to start classifying the object type. Short timescale.

## Outlier detection (WIP)
Short timescale.

## Light-curve inspection (WIP)
Long timescale.
