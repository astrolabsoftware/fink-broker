#!/bin/sh

SINFO="\xF0\x9F\x9B\x88"
SSTOP="\xF0\x9F\x9B\x91"
SDONE="\xE2\x9C\x85"

TABLE_PREFIX="rubin"

pre_split_three_digits() {
	# Define start and stop boundaries
	START=$1
	STOP=$2
	INCREMENT=$3

	# Initialize an empty array for SPLITS
	SPLITS=()

	# Loop through the range and populate the SPLITS array with two-digit numbers
	for ((i=$START; i<=$STOP; $INCREMENT)); do
	    # Format the number to two digits
	    SPLIT=$(printf "'%03d'" $i)
	    SPLITS+=($SPLIT)
	done

	# Convert the array to a comma-separated string
	SPLITS_STRING=$(IFS=,; echo "${SPLITS[*]}")

	# return split and number of partitions
	echo "$SPLITS_STRING" "${#SPLITS[@]}"
}

STANDARD_TABLES="${TABLE_PREFIX}.diaObject ${TABLE_PREFIX}.diaSource ${TABLE_PREFIX}.cutouts ${TABLE_PREFIX}.forcedSources ${TABLE_PREFIX}.pixel128"

for TABLE_NAME in $STANDARD_TABLES; do
	echo "Processing table $TABLE_NAME"
	if echo -e "list" | /opt/hbase/bin/hbase shell -n | grep $TABLE_NAME; then
		echo -e "$SSTOP Table $TABLE_NAME already exists -- not creating a new one."
	else
		echo -e "$SDONE $TABLE_NAME does not exists -- creating a new one"

		if [[ $TABLE_NAME == "${TABLE_PREFIX}.pixel128" ]]; then
			# pixel128 has a different splitting
			output=$(pre_split_three_digits 1000 199999 "i+=1000")
		else
			# Default splitting
			output=$(pre_split_three_digits 1 999 "i++")
		fi
		read -r SPLIT_POINTS NPARTS <<< "$output"
		echo -e "$SINFO LZ4 compression, 2 column families (i, d), $((NPARTS + 1)) partitions"
		COMMAND="create '${TABLE_NAME}', {NAME => 'i', COMPRESSION => 'LZ4'}, {NAME => 'd', COMPRESSION => 'LZ4'}, SPLITS=> [$SPLIT_POINTS]"

		# Create the table
		echo -e $COMMAND | /opt/hbase/bin/hbase shell -n
	fi
done

