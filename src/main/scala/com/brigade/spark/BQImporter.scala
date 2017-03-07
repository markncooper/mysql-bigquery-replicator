package com.brigade.spark

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.slf4j.LoggerFactory
import com.appsflyer.spark.bigquery._

import scala.collection.parallel.ForkJoinTaskSupport

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

  protected def getToday(): String = {
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
    val executor = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(4))

    val dbUtils = new DBUtils(config.getConfig("source-db"))
    val bqUtils = new BigQueryUtils(spark)

    val tablesToCopy = dbUtils.getTablesToSync()

    Logger.info("About to copy:")
    tablesToCopy.foreach { tableName =>
      Logger.info(s"\t$tableName")
    }

    val parallizedTablesToCopy = tablesToCopy.par
    parallizedTablesToCopy.tasksupport = executor

    parallizedTablesToCopy.foreach { tableMetadata =>

      val sourceDF =
        if (tableMetadata.getKeyColumnIsKnown() && tableMetadata.immutable && attemptIncrementalUpdates) {
          val bqCurrentMaxKeyValue = bqUtils.getMaxPrimaryKeyValue(mkFQTableName(tableMetadata.name), tableMetadata.keyColumnName.get)

          if (bqCurrentMaxKeyValue.isDefined) dbUtils.getSourceDF(tableMetadata.copy(minKey = bqCurrentMaxKeyValue))
          else dbUtils.getSourceDF(tableMetadata)
        } else {
          dbUtils.getSourceDF(tableMetadata)
        }

      saveToBigQuery(sourceDF, tableMetadata)
    }

    Logger.info("Done syncing tables.")
  }

  protected def saveToBigQuery(sourceDF: DataFrame, sourceTable: RDBMSTable): Unit = {
    val today = getToday()
    val appendWrite = sourceTable.getKeyColumnIsKnown() && sourceTable.immutable

    val writeDisposition =
      if (appendWrite && attemptIncrementalUpdates)
        {
          WriteDisposition.WRITE_APPEND
        } else {
          WriteDisposition.WRITE_TRUNCATE
        }

    val outputTableName =
      if (appendWrite && sourceTable.minKey.get!=0) s"${sourceTable.name}$$$today"
      else sourceTable.name

    Logger.info(s"Writing $outputTableName to BigQuery with $writeDisposition")

    sourceDF
      .saveAsBigQueryTable(
        mkFQTableName(outputTableName),
        appendWrite,
        writeDisposition,
        CreateDisposition.CREATE_IF_NEEDED
      )
  }
}

/**
  * Allows running as a stand-alone job or by linking to it from a wrapper job.
  */
object BQImporter {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()

    val spark = SparkSession
      .builder()
      .appName("BigQuery Importer")
      .getOrCreate()

    val importer = new BQImporter(spark, config)

    importer.run()
  }
}