# Apache Avro Alert Schema

Apache Avro uses a schema to structure the data (define data types and protocols) that is being encoded (compact binary format). It has two different types of schema languages: one for human editing (Avro IDL) and another which is more machine-readable based on (JSON).


## ZTF alert schema timeline

The ZTF alert schema evolves over time, to include the latest information. We provide a sample alert for each version (starting in 2019) in this folder: `template_schema_ZTF_<version>.avro`. One can find the latest description of fields on this [page](https://zwickytransientfacility.github.io/ztf-avro-alert/). 

| Period | Schema version | Change |
|:--------|:-------|:-------|
| Jul 2018 - Jan 2019 | version 3.1 | -- |
| Jan 2019 - Jul 2019 | version 3.2 | [commit](https://github.com/ZwickyTransientFacility/ztf-avro-alert/commit/2b4af549fc99200e3117c24634a17b5ac04ed963) |
| Jul 2019 - now | version 3.3 | [commit](https://github.com/ZwickyTransientFacility/ztf-avro-alert/commit/a4fa6a45621ccfc11e7a38f766a05c63681fd4e3#diff-c9550d5fad73447fc24ba47f95d1c6b7) |

## Fink Distribution schema [ZTF]

The Fink distribution schema is the input ZTF alert schema plus additional fields describing the added values by Fink. This schema is mandatory to decode the alerts receive by the Fink client, and we release schema versions in this folder: `distribution_schema_<version>.avsc`. The fields are described on this [page](https://fink-broker.readthedocs.io/en/latest/science/added_values/).

| Period | Schema version | Added values |
|:--------|:-------|:-------|
| Jul 2019 - now | version 0.1 | `cdsxmatch` |

## Troubleshooting

Using a wrong schema to decode alerts will lead to failures. Typically users will see this message:

```bash
TBD
```

Check you are using the relevant schema version according to the data you are processing. In fink-broker, you can always output the schema of an alert before it is sent by the broker using [get_kafka](https://github.com/astrolabsoftware/fink-broker/blob/master/fink_broker/distributionUtils.py#L29) and compare then to the schema you were using.