#!/bin/bash

mkdir -p results/outputs
mkdir -p results/logs

QUERY=$1
INPUT=$2
OUTPUT=$3

BASE="pipelines/mapreduce/$QUERY"

MAPPER="$BASE/mapper"

# compile mapper only
g++ $BASE/mapper.cpp -o $MAPPER

# ONLY MAP PHASE (no reduce here)
cat $INPUT | $MAPPER > $OUTPUT