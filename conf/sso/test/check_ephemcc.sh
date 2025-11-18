#!/bin/bash

EPHEMCC4=/opt/miriade/bin
OTYPE=asteroid
NAME=Julienpeloton
RPLANE=1
TCOOR=5
OBSERVER="I41"
TSCALE="UTC"
USERCONF=/tmp/.eproc-4.4
IOFILE=/tmp/default-ephemcc-observation.xml

# dates.txt has one column with 3 ISO times
$EPHEMCC4/ephemcc4.4 -u $USERCONF --iofile $IOFILE -tp $RPLANE -tc $TCOOR -c 3 -i $OBSERVER -e $TSCALE --jd -bl dates.txt -s file:json -f ephem-4.4.json -r ./ -t ssocard:$NAME
