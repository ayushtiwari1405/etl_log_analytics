# ETL Log Analytics System

## Overview
This project implements an ETL pipeline for analyzing NASA web server logs using a simulated MapReduce framework. It supports batching, multiple queries, SQL storage, reporting, and an interactive CLI-based user interface.

---

## Features
- Batch-based MapReduce execution  
- Multiple queries (q1, q2, q3)  
- Runtime tracking per dataset  
- PostgreSQL integration for storing results  
- Reporting module  
- Interactive CLI UI (Rich-based)  
- Support for multiple pipelines (MapReduce implemented, others extendable)  
- Flexible input selection (sample / Jul95 / Aug95 / both)

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

## Running Queries via CLI
```bash

# Run single query
python -m controller.main --pipeline mapreduce --query q1 --input sample

# Run with reporting
python -m controller.main --pipeline mapreduce --query q1 --input both --report
```

## Running via UI
```bash
python ui/main.py
```

## Flow:
``` 
Select Pipeline → Select Input → Select Query → Execute → View Report
```
