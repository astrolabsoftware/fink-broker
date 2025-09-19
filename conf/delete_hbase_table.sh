#!/bin/bash

TABLE_PREFIX="rubin"

STANDARD_TABLES="${TABLE_PREFIX}.diaObject ${TABLE_PREFIX}.ssObject ${TABLE_PREFIX}.diaSource_static ${TABLE_PREFIX}.diaSource_sso ${TABLE_PREFIX}.cutouts ${TABLE_PREFIX}.forcedSources ${TABLE_PREFIX}.pixel128"


for TABLE_NAME in $STANDARD_TABLES; do
        echo "Processing table $TABLE_NAME"
	COMMAND="disable '${TABLE_NAME}'"
	echo -e $COMMAND | /opt/hbase/bin/hbase shell -n

	COMMAND="drop '${TABLE_NAME}'"
	echo -e $COMMAND | /opt/hbase/bin/hbase shell -n
done

