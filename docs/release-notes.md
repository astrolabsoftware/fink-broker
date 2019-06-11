# 0.2.0

- Print version number from the binary (#132)
- Update the gitignore to stop tracking .coverage* files (#136)
- From raw database to science database (#140)
  - Connect to the raw database
  - Filter alerts based on instrumental or environmental criteria.
  - Classify alerts using the xMatch service at CDS.
- Add Raw database connector (#146)
- Hosting the data for the test suite (#152)
- Build the raw database before the unit test suite (#154)
- Flatten DataFrame columns before sending to the science database (#159)
- From stream to science (#162)
  - The data from the raw database is loaded (streaming), filtered, tagged and finally pushed to the HBase table (using a custom Spark Structured Streaming HBase connector).
  - **API break** (fink services renamed).
- Modified fink_test to allow any configuration file (#163)
- Install HBase for CI tests (#166)
- Changing the threshold for coverage (#169)
- Re-introduce the monitoring service (checkstream) (#172)
  - Was lost in #162
- New Alert Distribution module (#182)
  - read the science db (Hbase)
  - serialize into avro and publish to Kafka
  - a Kafka consumer to deserialize the avro message and do spark DF operations
- Updating Spark versions in the travis CI script (#183)
- Switch from custom HBase sink provider to foreachBatch mechanism (#184)
- Add a new xmatch tab to the dashboard (#186)
- Bump HBase version for the CI to 2.1.5 (#188)
- Repo management: setup and new structure for the python module (#191)
  - **API break** (python module path changed)
- Several documentation and README updates (#134, #138, #142, #149, #173, #177)

Thanks to @cAbhi15 and @tallamjr who contribute to this new release!

# 0.1.1

* Change python to python3 in the fink binary when launching the simulator ([#93](https://github.com/astrolabsoftware/fink-broker/pull/93) )
* Split Spark extra option and Kafka parameters in configuration files ([#94](https://github.com/astrolabsoftware/fink-broker/pull/94) )
* Remove information on IP and topic ([#95](https://github.com/astrolabsoftware/fink-broker/pull/95) )
* Updating path to Spark binary ([#105](https://github.com/astrolabsoftware/fink-broker/pull/105) )
* Test against multiple versions of Apache Spark ([#107](https://github.com/astrolabsoftware/fink-broker/pull/107) )
* Switch to encrypted token for SonarQube ([#108](https://github.com/astrolabsoftware/fink-broker/pull/108)) )
* Add section in CONTRIBUTING about naming PR branch ([#112](https://github.com/astrolabsoftware/fink-broker/pull/112) )
* Updating travis.yml file to allow for user agnostic CI tests ([#114](https://github.com/astrolabsoftware/fink-broker/pull/114) )
* Updating gitignore ([#115](https://github.com/astrolabsoftware/fink-broker/pull/115) )
* Disable sonar analysis for PR from fork ([#118](https://github.com/astrolabsoftware/fink-broker/pull/118) )
* Connect to earliest offsets in tests ([#121](https://github.com/astrolabsoftware/fink-broker/pull/121))
* Long-term performance: storage and display ([#122](https://github.com/astrolabsoftware/fink-broker/pull/122) )
* Fix Kafka retention period ([#124](https://github.com/astrolabsoftware/fink-broker/pull/124))
* Add templates for Issues and PR in GitHub ([#129](https://github.com/astrolabsoftware/fink-broker/pull/129))

Thanks to @tallamjr for many contributions!

# 0.1.0

This is the first stable release of Fink. Fink building bricks are:

* infrastructure
* database
* services
* dashboard

Usage is defined in https://fink-broker.readthedocs.io/en/latest/
