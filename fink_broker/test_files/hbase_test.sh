#!/bin/bash

set -e

if [[ $1 == "--create" ]]; then
  exist=$( echo list | hbase shell -n | grep 'test_table' | awk 'NR==1{ print $1 }' )

  # if table doesn't exist already
  if [ -z "$exist" ]; then
    # create table
    echo -e "create 'test_table', 'i'" | hbase shell -n
  fi

  # add rows to the table
  echo -e "put 'test_table', 'ZTF18aceatkx', 'i:simbadType', 'Star'" | hbase shell -n
  echo -e "put 'test_table', 'ZTF18aceatkx', 'i:status', 'updateDB'" | hbase shell -n

  echo -e "put 'test_table', 'ZTF18acsbjvw', 'i:simbadType', 'Unknown'" | hbase shell -n
  echo -e "put 'test_table', 'ZTF18acsbjvw', 'i:status', 'updateDB'" | hbase shell -n

  echo -e "put 'test_table', 'ZTF18acsbten', 'i:simbadType', 'Unknown'" | hbase shell -n
  echo -e "put 'test_table', 'ZTF18acsbten', 'i:status', 'updateDB'" | hbase shell -n

elif [[ $1 == "--delete" ]]; then
  exist=$( echo list | hbase shell -n | grep 'test_table' | awk 'NR==1{ print $1 }' )

  # if table doesn't exist
  if [ -z "$exist" ]; then
    # create table
    echo -e "table doesn't exist\n"
  else
    # diable the table
    echo -e "disable 'test_table'" | hbase shell -n
    
    # drop the table
    echo -e "drop 'test_table'" | hbase shell -n
  fi
fi
