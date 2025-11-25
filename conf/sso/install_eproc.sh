#!/bin/bash

TARNAME=$1
FOLDER=$2

# remove if existing /opt/miriade
if [ -d "$(readlink -f /opt/miriade)" ]; then
    rm /opt/miriade
fi

# uncompress
tar xzvf /opt/$TARNAME -C /

# create the symlink
ln -s $FOLDER /opt/miriade

# remove archive
rm -rf /opt/$TARNAME
