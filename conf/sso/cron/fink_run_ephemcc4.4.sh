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

# resolve the name
response=$(curl https://api.ssodnet.imcce.fr/quaero/1/sso/search?q=$NAME)

# Use jq to extract the id
id=$(echo "$response" | jq -r '.data[0].id')

# Print the id
echo "$id"

# For the epehe
$EPHEMCC4/ephemcc4.4 \
    -u $USERCONF \
    --iofile $IOFILE \
    -t ssocard:$id \
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
