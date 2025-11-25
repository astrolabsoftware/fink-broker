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
# See https://ssp.imcce.fr/webservices/ssodnet/api/quaero/
types=("Asteroid" "Comet" "Planet" "Dwarf%20Planet" "Interstellar%20Object")
id=null

# Loop through each type
for type in "${types[@]}"; do
    # echo "Checking for ${type}"

    # resolve the name
    response=$(curl -s "https://api.ssodnet.imcce.fr/quaero/1/sso?q=${ENCODED_NAME}&type=${type}")

    # Use jq to extract the id
    id=$(echo "$response" | jq -r '.data[0].id')

    # Check if id is not null
    if [[ "$id" != "null" && -n "$id" ]]; then
        break  # Exit the loop if a valid id is found
    fi
done

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
