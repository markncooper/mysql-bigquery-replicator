package com.brigade.spark

import java.io.IOException
import java.util.concurrent.ExecutionException

import org.apache.spark.sql.{SQLContext, SparkSession}
import com.appsflyer.spark.bigquery._
import org.slf4j.LoggerFactory

class BigQueryUtils(spark: SparkSession) {
  val Logger = LoggerFactory.getLogger(getClass)
  import spark.implicits._
  val sqlContext = spark.sqlContext

  def getMaxPrimaryKeyValue(tableName: String, tableColumn: String): Option[Long] = {
    try {
      Logger.info(s"Getting max value for key column $tableColumn on table $tableName")
      val results = sqlContext.bigQuerySelect(s"select max($tableColumn) as maxval from [$tableName]")

      results.map { row =>
        row.getAs[String]("maxval").toLong
      }.collect().headOption
    } catch {
      case ioe: ExecutionException => {
        Logger.warn(s"Table $tableName doesn't exist in BigQuery; is the initial load of it?")
        None
      }
    }
  }
}
