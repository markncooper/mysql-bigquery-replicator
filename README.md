# mysql-bigquery-replicator

A simple library that ease the task of writing snapshots of Mysql databases to BigQuery

Usage:

> sbt assembly

> spark-submit --files your_credentials.json --class com.brigade.spark.BQImporter target/scala-2.11/mysql-bigquery-replicator-assembly-0.jar