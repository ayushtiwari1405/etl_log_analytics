# ETL Log Analytics System

## Overview
This project implements an ETL pipeline for analyzing NASA web server logs using a simulated MapReduce framework. It supports batching, multiple queries, SQL storage, and reporting.

---

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Download dataset
```bash
mkdir -p data/raw
cd data/raw
wget https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
wget https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz
gunzip *.gz
cd ../..
```

## Running MapReduce Pipeline
```bash
# Run single query
python -m controller.main --pipeline mapreduce --query q1

# Run with reporting
python -m controller.main --pipeline mapreduce --query q1 --report

# Run all queries
chmod +x run_all.sh
./run_all.sh
```