#!/bin/bash

echo "=============================="
echo "Running MapReduce Pipeline"
echo ""
echo ">>> Running Query 1 (Daily Traffic Summary)"
python -m controller.main --pipeline mapreduce --query q1 --report

echo ""
echo ">>> Running Query 2 (Top Resources)"
python -m controller.main --pipeline mapreduce --query q2 --report

echo ""
echo ">>> Running Query 3 (Hourly Error Analysis)"
python -m controller.main --pipeline mapreduce --query q3  --report

echo ""
echo "All queries completed"
echo "=============================="
