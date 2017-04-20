package com.brigade.spark

import bqutils.BigQueryServiceFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.google.api.services.bigquery.Bigquery
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration
import org.slf4j.LoggerFactory

import com.spotify.spark.bigquery._

class BrigadeBigQueryUtils(spark: SparkSession, projectName: String, datasetName: String, gcsTempBucket: String, keyFile: String) extends Serializable {
  private val Logger = LoggerFactory.getLogger(getClass)
  @transient private val sqlContext = spark.sqlContext
  @transient private lazy val hadoopConf = spark.sqlContext.sparkContext.hadoopConfiguration
  @transient private val bigquery: Bigquery = BigQueryServiceFactory.getService

  hadoopConf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectName)
  hadoopConf.set("fs.gs.project.id", projectName)
  hadoopConf.set(BigQueryConfiguration.GCS_BUCKET_KEY, gcsTempBucket)
  hadoopConf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectName)

  sqlContext.setGcpJsonKeyFile(keyFile)

  def saveToBigquery(dataframe: DataFrame, filename: String): Unit = {
    val fqTableName = s"$projectName:$datasetName.$filename"
    dataframe.printSchema()
    dataframe.saveAsBigQueryTable(fqTableName, WriteDisposition.WRITE_TRUNCATE)
  }

  private def delete(path: Path): Unit = {
    val fs = FileSystem.get(path.toUri, hadoopConf)
    fs.delete(path, true)
  }
}
