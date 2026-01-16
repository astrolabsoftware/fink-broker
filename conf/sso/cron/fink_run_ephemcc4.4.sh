#!/bin/bash

source ~/.bash_profile


export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/miriade/lib

EPHEMCC4=/opt/miriade/bin
OTYPE=aster
NAME="$1"
RPLANE=$2
TCOOR=$3
OBSERVER=$4
TSCALE=$5
USERCONF=$6
IOFILE=$7
RAMPATH=$8
UIDD=$9

# encode the name
ENCODED_NAME="${NAME// /%20}"

# Define a list of types
id=null

types="Asteroid OR %22Dwarf planet%22 OR Comet OR %22Interstellar Object%22"
types="${types// /%20}"
[[ ${NAME} =~ ^[0-9]+$ ]] && qstring="aliases.raw:${NAME}" || qstring="${NAME}"
qstring="${qstring}%20AND%20type:(${types})"

# resolve the name
response=$(curl -s "https://api.ssodnet.imcce.fr/quaero/1/sso/search?q=${qstring}")

# Use jq to extract the id
id=$(echo "$response" | jq -r '.data[0].id')

id=$(printf '%s' "$id" | jq -sRr @uri)

# For the epehe
$EPHEMCC4/ephemcc4.4 \
    -u $USERCONF \
    --iofile $IOFILE \
    -t ssocard:"$id" \
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
