#!/bin/sh

# run unit tests inside fink_broker container
pytest $FINK_HOME --capture=tee-sys -vv

