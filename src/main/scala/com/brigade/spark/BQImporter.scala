package com.brigade.spark

import com.appsflyer.spark.bigquery._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

class BQImporter(spark: SparkSession, jsonKeyFile: String) {
  @transient lazy val Logger = LoggerFactory.getLogger(getClass)

  def run() = {
    Logger.info("Starting BQ importer.")

    val sqlContext = spark.sqlContext

    val sourceTableDF = sqlContext
      .read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "<DB URL>")
      .option("dbtable", "<DB TABLE>")
      .option("pushdown", "true")
      .load()

    sourceTableDF.printSchema()


    sqlContext.setGcpJsonKeyFile(jsonKeyFile)
    sqlContext.setBigQueryProjectId("<PROJECT ID>")
    sqlContext.setBigQueryGcsBucket("<TEMP BUCKET>")
    sqlContext.setGSProjectId("<PROJECT ID>")
  }
}

object BQImporter {
  def main(args: Array[String]): Unit = {
    println("Hi.")

    val spark = SparkSession
      .builder()
      .appName("BigQuery")
      .getOrCreate()

    val importer = new BQImporter(spark, "")

    importer.run()
  }
}