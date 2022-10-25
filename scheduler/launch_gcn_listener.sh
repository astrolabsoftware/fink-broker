#!/bin/bash

mkdir -p $HOME/fink_grb_out

# should be up 24/7
nohup fink_grb gcn_stream start --config $FINK_HOME/conf_cluster/fink_grb.conf > $HOME/fink_grb_out/gcn_stream.log &
