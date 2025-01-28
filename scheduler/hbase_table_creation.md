# HBase tables 

HBase tables need to be generated with correct parameters before pushing data. There are two types of table: cube and sheet. Cubic table uses versioning as 3rd dimension, while flat tables purely uses indexing.

## rubin.object

**Purpose:** This table contains summary/aggregated information about an object. From Rubin, this corresponds to the entry `diaObject.*`. From Fink, this is to be defined, but we typically should push any high level description of objects (features, rates, etc.)
 
**Structure:** The table is cubic. It is indexed by `diaObjectId`, and contains as many versions as necessary. The table is compressed.

```bash
#!/bin/sh

TABLE_NAME="rubin.object"
COMPRESSION="GZ"

COMMAND="create '${TABLE_NAME}',\
     {NAME => 'i', VERSIONS => 50000, COMPRESSION => '${COMPRESSION}'},\  # cube
     {NAME => 'd', VERSIONS => 50000, COMPRESSION => '${COMPRESSION}'}"  # cube

echo -e ${COMMAND} | /opt/hbase/bin/hbase shell -n
```
