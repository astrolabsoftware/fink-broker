#!/bin/bash

source ~/.bash_profile

fink_db -s rubin --merge

# Not need anymore in the current scheduler
# but uncomment if you want to re-ingest alerts in bulk
# fink_db -s rubin --main_table

fink_db -s rubin --index_tables
