#!/bin/bash

# Set PySpark environment variables (adjust paths as needed)
export SPARK_HOME=/path/to/spark
export PATH=$SPARK_HOME/bin:$PATH
export PYSPARK_PYTHON=python3

# Execute PySpark scripts
spark-submit /home/ubuntu/data_ingestion.py
spark-submit /home/ubuntu/data_process.py
spark-submit /home/ubuntu/data_sql.py

# Execute AWS CLI command
aws s3 sync /home/ubuntu/calihealth/processed_data/ s3://calihealthcares3/processed_data/