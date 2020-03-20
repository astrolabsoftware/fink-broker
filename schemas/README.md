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

| Period | Schema version | Commit | Added values |
|:--------|:-------|:-------|:-------|
| Jul 2019 - Jan 2020 | version 0.1 | -- | `cdsxmatch` |
| Jan 2020 - now | version 0.2 | [commit](https://github.com/astrolabsoftware/fink-broker/commit/bc5a03ae42513841c8c071a49f17bae1978e0e94) | `rfscore` |

## Simulator vs live streams

We run Fink in two modes: live (i.e. live data from ZTF), or simulation (replayed
streams using the Fink alert simulator). While we would expect the schema to be the same,
there are some small variations due to the way Spark handles input schema. The variations are as small as some missing comments in the schema - but nevertheless sufficient for the client to consider the data from live or simulation to need a different schema... Hence, for each schema version, we have two files:

```
schema_name_XpY.avsc --> suitable for data from simulator
schema_name_XpY-live.avsc --> suitable for live data
```

We intend to find a solution to merge the two in the future.

## Troubleshooting

Using a wrong schema to decode alerts will lead to failures. Typically broker users will see this message:

```bash
Caused by: org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 0.0 failed 1 times, most recent failure: Lost task 0.0 in stage 0.0 (TID 0, localhost, executor driver): java.lang.ArrayIndexOutOfBoundsException: -28

	at org.apache.avro.io.parsing.Symbol$Alternative.getSymbol(Symbol.java:424)

	at org.apache.avro.io.ResolvingDecoder.doAction(ResolvingDecoder.java:290)

	at org.apache.avro.io.parsing.Parser.advance(Parser.java:88)

	at org.apache.avro.io.ResolvingDecoder.readIndex(ResolvingDecoder.java:267)

	at org.apache.avro.generic.GenericDatumReader.readWithoutConversion(GenericDatumReader.java:179)
	...
```

and fink-client users could see:

```bash
In [3]: topic, alert = consumer.poll(timeout=1)
---------------------------------------------------------------------------
UnicodeDecodeError                        Traceback (most recent call last)
<ipython-input-4-d5d6e0affb36> in <module>
----> 1 topic, alert = consumer.poll(timeout=ts)

~/Documents/workspace/myrepos/fink-client/fink_client/consumer.py in poll(self, timeout)
     92         topic = msg.topic()
     93         avro_alert = io.BytesIO(msg.value())
---> 94         alert = _decode_avro_alert(avro_alert, self._parsed_schema)
     95
     96         return topic, alert

~/Documents/workspace/myrepos/fink-client/fink_client/consumer.py in _decode_avro_alert(avro_alert, schema)
    234     """
    235     avro_alert.seek(0)
--> 236     return fastavro.schemaless_reader(avro_alert, schema)

fastavro/_read.pyx in fastavro._read.schemaless_reader()

fastavro/_read.pyx in fastavro._read.schemaless_reader()

fastavro/_read.pyx in fastavro._read._read_data()

fastavro/_read.pyx in fastavro._read.read_record()

fastavro/_read.pyx in fastavro._read._read_data()

fastavro/_read.pyx in fastavro._read.read_union()

fastavro/_read.pyx in fastavro._read._read_data()

fastavro/_read.pyx in fastavro._read.read_utf8()

fastavro/_six.pyx in fastavro._six.py3_btou()

UnicodeDecodeError: 'utf-8' codec can't decode byte 0xc2 in position 13: invalid continuation byte
```

Check you are using the relevant schema version according to the data you are processing. In fink-broker, you can always output the schema of an alert before it is sent by the broker using [get_kafka](https://github.com/astrolabsoftware/fink-broker/blob/master/fink_broker/distributionUtils.py#L29) and compare then to the schema you were using.
