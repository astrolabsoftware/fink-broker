#!/bin/bash

TARNAME=miriade.tar.gz

# uncompress
tar xzvf /opt/$TARNAME -C /

# remove archive
rm -rf /opt/$TARNAME
