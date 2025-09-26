#!/bin/bash

source ~/.bash_profile

fink_db -s rubin --merge
fink_db -s rubin --main_table
fink_db -s rubin --index_tables
