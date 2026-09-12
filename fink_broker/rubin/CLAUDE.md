# fink_broker/rubin

Rubin/LSST-specific processing: science modules (`science.py`), alert decoding
(`decoding_utils.py`, Kafka/Avro/Parquet), HBase schemas (`hbase_utils.py`), Spark helpers
(`spark_utils.py`).

## Rules

- **Never import from `fink_broker/ztf/`.** Shared logic goes to `fink_broker/common/`.
- Spark jobs that use this module live in `bin/rubin/` (`stream2raw.py`, `raw2science.py`,
  `distribute.py`, `merge.py`, `extract_schema.py`, `fetch_schema.py`). Config: `conf/rubin/`.
  Scheduling: `scheduler/rubin/`.
- Science modules come from the external `fink-science` package (`fink_science.rubin.*`) — check
  there before adding scientific logic here.
- Alert schemas are versioned in `rubin_parquet_schema/`; a schema change is a compatibility
  decision, not an implementation detail — flag it rather than silently adapting the code.

## Tests

Doctests in numpy `Examples` sections, run by the `__main__` block
(`regular_unit_tests(globals())` / `spark_unit_tests(globals())`), executed by
`bin/fink_test_rubin --unit-tests`.
