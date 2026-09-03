# fink_broker/ztf

ZTF-specific processing: science modules (`science.py`), HBase schemas (`hbase_utils.py`),
multi-messenger / GRB joins (`mm_utils.py`, via `fink_mm`), tracklet identification
(`tracklet_identification.py`).

## Rules

- **Never import from `fink_broker/rubin/`.** Shared logic goes to `fink_broker/common/`.
- Spark jobs that use this module live in `bin/ztf/` (`stream2raw.py`, `raw2science.py`,
  `distribute.py`, `merge.py`, the `archive_*.py` family). Config: `conf/ztf/`.
  Scheduling: `scheduler/ztf/`.
- Science modules come from the external `fink-science` package (`fink_science.ztf.*`) and `fink-filters` — check there
  before adding scientific logic here.

## Tests

Doctests in numpy `Examples` sections, run by the `__main__` block
(`regular_unit_tests(globals())` / `spark_unit_tests(globals())`), executed by
`bin/fink_test_ztf --unit-tests`. `test_schema_converter.py` is the pytest-based exception.
