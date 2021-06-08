# DATA LAKE ELT

This project involved creating a Data Lake by loading files from S3 buckets
transforming the data into different dimensional tables stored on S3.

The datasets include:
1. Song data
2. Log data

The schema tables

1. Fact table: 
    - Songplays

2. Dimension tables:
    - Users
    - Artists
    - Time
    - Songs

## Files in Repo

4 files were used for this project, apart from the README.

1. *etl.py*: This is the main file. All the ELT steps are defined here.
    
2. *dl.cfg*: This file contains all the configuration variables used in the different scripts

## Usage

The pipeline is designed to be run on the EMR master node [it is assumed that the EMR cluster has been created].
The results are written to the output folder "datalake_etl_output" in S3.

```bash
spark-submit etl.py
```

## NOTE
All cluster and database details have been removed from dl.cfg for security reasons.
The assumption is that you already have a running EMR cluster.
