package com.brigade.spark

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.slf4j.LoggerFactory
import com.appsflyer.spark.bigquery._
import com.google.common.base.Splitter

class BQImporter(spark: SparkSession, config: Config) {
  @transient val Logger = LoggerFactory.getLogger(getClass)
  implicit private val sqlContext = new SQLContext(spark.sparkContext)
  private val gcpConfig = config.getConfig("gcp")

  private val gcpProjectID = gcpConfig.getString("project-id")
  private val gcpTempBucketID = gcpConfig.getString("tempbucket-id")
  private val gcpDatasetID = gcpConfig.getString("dataset-id")
  private val gcpOverwriteExisting = gcpConfig.getBoolean("overwrite-existing")
  private val maxParallelWriters = gcpConfig.getInt("max-parallel-writers")
  private val gcpTablePrefix = gcpConfig.getString("table-prefix")
  private val gcpJsonCredentialsFilename = gcpConfig.getString("json-credentials")
  private val attemptIncrementalUpdates = config.getBoolean("attempt-incremental-updates")

  def getToday(): String = {
    val now = LocalDate.now()
    val fmt = DateTimeFormatter.ofPattern("yyyyMMdd")
    now.format(fmt)
  }

  def mkFQTableName(table: String) = s"$gcpProjectID:$gcpDatasetID.$gcpTablePrefix$table"

  def run() = {
    Logger.info("Starting BQ importer.")

    sqlContext.setGcpJsonKeyFile(gcpJsonCredentialsFilename)
    sqlContext.setBigQueryProjectId(gcpProjectID)
    sqlContext.setBigQueryGcsBucket(gcpTempBucketID)
    sqlContext.setGSProjectId(gcpProjectID)

    val dbUtils = new DBUtils(config.getConfig("source-db"))
    val bqUtils = new BigQueryUtils(spark)

    val tableToCopy = dbUtils.getTablesToSync()

    Logger.info("About to copy:")
    tableToCopy.foreach { tableName =>
      Logger.info(s"\t$tableName")
    }

    tableToCopy.foreach { table =>
      println(table)
    }

    tableToCopy.foreach { table =>

      val sourceDF =
        if (table.getKeyColumnIsKnown() && table.immutable && attemptIncrementalUpdates) {
          val bqCurrentMaxKeyValue = bqUtils.getMaxPrimaryKeyValue(mkFQTableName(table.name), table.keyColumnName.get)

          if (bqCurrentMaxKeyValue.isDefined) dbUtils.getSourceDF(table.copy(minKey = bqCurrentMaxKeyValue))
          else dbUtils.getSourceDF(table)
        } else {
          dbUtils.getSourceDF(table)
        }

      val appendWrite = table.getKeyColumnIsKnown() && table.immutable

      val writeDisposition
        = if (appendWrite)
          {
            WriteDisposition.WRITE_APPEND
          } else {
            WriteDisposition.WRITE_TRUNCATE
          }

      val today = getToday()

      val outputTableName =
        if (appendWrite && table.minKey.get!=0) s"${table.name}$$$today"
        else table.name

      sourceDF
        .saveAsBigQueryTable(
          mkFQTableName(outputTableName),
          appendWrite,
          writeDisposition,
          CreateDisposition.CREATE_IF_NEEDED,
          true
        )
    }

    Logger.info("Done syncing tables.")
  }
}

object BQImporter {
  def main(args: Array[String]): Unit = {
    val s = Splitter.on(',').splitToList("1,3,4")

    val config = ConfigFactory.load()

    val spark = SparkSession
      .builder()
      .appName("BigQuery Importer")
      .getOrCreate()

    val importer = new BQImporter(spark, config)

    importer.run()
  }
}