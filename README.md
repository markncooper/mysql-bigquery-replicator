# mysql-bigquery-replicator

A simple library that eases the task of writing snapshots of Mysql databases to BigQuery. It uses the Avro format
so while the wire format is compact, you lose some schema richness.

## First configure the tool

Edit reference.conf - Drop the appropriate settings into that file.

## Building mysql-bigquery-replicator:

    sbt assembly

Once assembly has been run, you'll have a fat jar with the config bundled in it.

## Running

    spark-submit --files your_credentials.json --class com.brigade.spark.BQImporter target/scala-2.11/mysql-bigquery-replicator-assembly-0.jar

## Other info

[![Build Status](https://travis-ci.org/markncooper/mysql-bigquery-replicator.svg?branch=master)](https://travis-ci.org/markncooper/mysql-bigquery-replicator)

