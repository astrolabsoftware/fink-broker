#!/bin/bash

source ~/.bash_profile

fink_db -s ztf --merge
fink_db -s ztf --main_table
fink_db -s ztf --index_tables
fink_db -s ztf --clean_night
