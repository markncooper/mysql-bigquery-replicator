package com.brigade.spark

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.parallel.ForkJoinTaskSupport


class BQImporter(spark: SparkSession, config: ImportConfig) {
  @transient val Logger = LoggerFactory.getLogger(getClass)
  implicit private val sqlContext = new SQLContext(spark.sparkContext)

  /*
  private val gcpConfig = config.getConfig("gcp")
  private val gcpProjectID = gcpConfig.getString("project-id")
  private val gcpTempBucketID = gcpConfig.getString("tempbucket-id")
  private val gcpDatasetID = gcpConfig.getString("dataset-id")
  private val gcpOverwriteExisting = gcpConfig.getBoolean("overwrite-existing")
  private val maxParallelWriters = config.getInt("max-parallel-writers")
  private val gcpTablePrefix = gcpConfig.getString("table-prefix")
  private val gcpJsonCredentialsFilename = System.getenv().get("GOOGLE_APPLICATION_CREDENTIALS")
  private val attemptIncrementalUpdates = config.getBoolean("attempt-incremental-updates")
  private val maxRetries = config.getInt("max-retries")
*/

  @transient private val bqUtils = new BrigadeBigQueryUtils(
    spark,
    config.projectId,
    config.datasetId,
    config.tempBucketId,
    config.credentialsFilename
  )

  protected def getToday(): String = {
    val now = LocalDate.now()
    val fmt = DateTimeFormatter.ofPattern("yyyyMMdd")
    now.format(fmt)
  }

  def run() = {
    Logger.info("Starting BQ importer.")

    val executor = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(config.maxParallelWriters))
    val dbUtils = new DBUtils(config)
    val tablesToCopy = dbUtils.getTablesToSync()

    Logger.info("About to copy:")
    tablesToCopy.foreach { tableName =>
      Logger.info(s"\t$tableName")
    }
    Logger.info(s"Into dataset ${config.datasetId}.")

    val parallizedTablesToCopy = tablesToCopy.par
    parallizedTablesToCopy.tasksupport = executor

    val failedTables =
      parallizedTablesToCopy.flatMap { tableMetadata =>

        val sourceDF =
          if (tableMetadata.getKeyColumnIsKnown() && tableMetadata.immutable && config.attemptIncrementalUpdates) {
            throw new RuntimeException("Not implemented yet.")
          } else {
            dbUtils.getSourceDF(tableMetadata)
          }

        saveToBigQuery(sourceDF, tableMetadata)
      }

    if (failedTables.nonEmpty) {
      Logger.error(s"Failed to copy the following tables: ${failedTables.mkString(", ")}")
    }

    Logger.info("Done syncing tables.")
  }

  /**
    * Write the data frame to Google Cloud Storage and then kick off a job that loads that data into BigQuery.
    * @param sourceDF
    * @param sourceTable
    * @return
    */
  protected def saveToBigQuery(sourceDF: DataFrame, sourceTable: RDBMSTable): Option[String] = {
    val outputTableName = sourceTable.name

    var gcpPath = ""

    try {
      retry(config.maxRetries, outputTableName) {
        bqUtils.saveToBigquery(sourceDF, config.outputTablePrefix + outputTableName)
      }
      None
    } catch {
      case e: Exception => {
        Logger.error(s"Failed to load ${outputTableName} from $gcpPath")
        Some(outputTableName)
      }
    }
  }

  /**
    * Simple retry wrapper.
    */
  private def retry(n: Int, errorInfo: String)(fn: => Unit): Unit = {
    try {
      fn
    } catch {
      case e: Throwable =>
        if (n > 1) {
          Logger.error(s"An exception occurred, retrying ${errorInfo}.", e)
          retry(n - 1, errorInfo)(fn)
        }
        else {
          throw new RuntimeException("Too many retries, giving up.", e)
        }
    }
  }
}

/**
  * Allows running as a stand-alone job or by linking to it from a wrapper job.
  */
object BQImporter {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val importConfig = ImportConfig(config)

    val spark = SparkSession
      .builder()
      .appName("BigQuery Importer")
      .getOrCreate()

    val importer = new BQImporter(spark, importConfig)

    importer.run()
  }
}
