# mysql-bigquery-replicator

A simple library that eases the task of writing snapshots of Mysql databases to BigQuery. It uses the Avro format
so while the wire format is compact, though you lose some schema richness. It's very much an in-need-of-cleanup
alpha tool so use at your own risk.

## First configure the tool

Edit reference.conf - Drop the appropriate settings into that file.

## Set your GCP keyfile name

    export GOOGLE_APPLICATION_CREDENTIALS=gcp-prod-credentials.json

## Building mysql-bigquery-replicator:

    sbt assembly

Once assembly has been run, you'll have a fat jar with the config bundled in it.

## Running

    spark-submit --files gcp-prod-credentials.json --class com.brigade.spark.BQImporter target/scala-2.11/mysql-bigquery-replicator-assembly-0.jar

## Example reference.conf

```
source-db {
  url = "jdbc:mysql://host/database"
  user = "my_robot_user"
  password = "some_password"
  database = "my_db"
  tables-whitelist = [table1, table2, table3]
  tables-blacklist = []
  auto-parallelize = true
  megabytes-per-partition = 300
}

gcp {
  project-id = "-media"
  tempbucket-id = "my-temp-bucket"
  dataset-id = "some_dataset"
  create-if-absent = true
  overwrite-existing = true
  table-prefix = "dave_"
}

attempt-incremental-updates = false
max-parallel-writers = 4
max-retries = 2
```


## Build Status

[![Build Status](https://travis-ci.org/markncooper/mysql-bigquery-replicator.svg?branch=master)](https://travis-ci.org/markncooper/mysql-bigquery-replicator)
