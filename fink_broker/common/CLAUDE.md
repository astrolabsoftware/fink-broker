# fink_broker/common

Code shared by **all** surveys: Spark session and streaming helpers (`spark_utils.py`), Avro
handling (`avro_utils.py`), HBase access (`hbase_utils.py`), Kafka distribution
(`distribution_utils.py`), logging (`logging_utils.py`), CLI parsing (`parser.py`), night
resolution (`night_utils.py`), partitioning (`partitioning.py`), test harness (`tester.py`).

## Rules

- **Never import from `fink_broker/ztf/` or `fink_broker/rubin/`.** The dependency goes one way:
  surveys depend on `common`, never the reverse.
- Any logic needed by both ZTF and Rubin belongs here. If you are about to copy a function from one
  survey module to the other, move it here instead — and say so, it changes both surveys.
- A change here affects every survey. Check both `ztf/` and `rubin/` callers before altering a
  signature or a return type.

## Tests

Doctests in numpy `Examples` sections, run by the `__main__` block at the end of each module
(`regular_unit_tests(globals())`, or `spark_unit_tests(globals())` for Spark code). `test_*.py`
files exist for cases that need fixtures (`test_night_utils.py`).
