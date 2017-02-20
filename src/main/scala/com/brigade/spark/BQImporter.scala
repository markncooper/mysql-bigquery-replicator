package com.brigade.spark

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.slf4j.LoggerFactory
import com.appsflyer.spark.bigquery._

class BQImporter(spark: SparkSession, config: Config) {
  @transient lazy val Logger = LoggerFactory.getLogger(getClass)
  val sqlContext = new SQLContext(spark.sparkContext)
  val gcpConfig = config.getConfig("gcp")

  val gcpProjectID = gcpConfig.getString("project-id")
  val gcpTempBucketID = gcpConfig.getString("tempbucket-id")
  val gcpDatasetID = gcpConfig.getString("dataset-id")
  val gcpOverwriteExisting = gcpConfig.getBoolean("overwrite-existing")
  val maxParallelWriters = gcpConfig.getInt("max-parallel-writers")
  val gcpTablePrefix = gcpConfig.getInt("table-prefix")
  val gcpJsonCredentialsFilename = gcpConfig.getString("gcp-json-credentials")

  def run() = {
    Logger.info("Starting BQ importer.")

    sqlContext.setGcpJsonKeyFile(gcpJsonCredentialsFilename)
    sqlContext.setBigQueryProjectId(gcpProjectID)
    sqlContext.setBigQueryGcsBucket(gcpTempBucketID)
    sqlContext.setGSProjectId(gcpProjectID)

    val dbUtils = new DBUtils(config.getConfig("source-db"), spark.sqlContext)

    dbUtils.getTablesToSync().foreach { table =>
      val sourceDF = dbUtils.getConnection(table.name)

      sourceDF
        .repartition(maxParallelWriters)
        .saveAsBigQueryTable(
          s"gcpProjectID:gcpDatasetID.$gcpTablePrefix${table.name}",
          false,
          WriteDisposition.WRITE_TRUNCATE,
          CreateDisposition.CREATE_IF_NEEDED
        )
    }

    Logger.info("Done syncing tables.")
  }
}

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