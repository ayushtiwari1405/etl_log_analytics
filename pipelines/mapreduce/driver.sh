#!/bin/bash

mkdir -p results/outputs
mkdir -p results/logs

QUERY=$1
INPUT=$2
OUTPUT=$3

BASE="pipelines/mapreduce/$QUERY"

MAPPER="$BASE/mapper"
REDUCER="$BASE/reducer"

g++ $BASE/mapper.cpp -o $MAPPER
g++ $BASE/reducer.cpp -o $REDUCER

if [ "$QUERY" = "q2" ]; then
    cat $INPUT | $MAPPER | sort | $REDUCER | sort -k2,2nr -k3,3nr | head -20 > $OUTPUT
else
    cat $INPUT | $MAPPER | sort | $REDUCER > $OUTPUT
fi
