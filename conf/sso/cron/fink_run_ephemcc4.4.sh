#!/bin/bash

source ~/.bash_profile


export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/miriade/lib

EPHEMCC4=/opt/miriade/bin
OTYPE=aster
NAME=$1
RPLANE=$2
TCOOR=$3
OBSERVER=$4
TSCALE=$5
USERCONF=$6
IOFILE=$7
RAMPATH=$8
UIDD=$9

# For the epehe
$EPHEMCC4/ephemcc4.4 \
    -u $USERCONF \
    --iofile $IOFILE \
    -t ssocard:$NAME \
    -tp $RPLANE \
    -tc $TCOOR \
    -c 3 \
    -i $OBSERVER \
    -e $TSCALE \
    --jd \
    -bl $RAMPATH/dates_$UIDD.txt \
    -s file:json \
    -f ephem_$UIDD.json \
    -r $RAMPATH
