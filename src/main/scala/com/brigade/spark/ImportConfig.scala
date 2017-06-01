package com.brigade.spark

import com.typesafe.config.Config
import scala.collection.JavaConverters._
import com.brigade.spark.ImportConfig._

/**
  *
  * @param projectId The GCP project ID
  * @param tempBucketId The temp bucket to use to move files into BigQuery
  * @param datasetId The dataset ID of your target
  * @param credentialsFilename The path to your credentials file
  * @param outputTablePrefix A prefix to prepend to your table names
  * @param overwriteExisting Overwrite existing tables if they already exist?
  * @param maxRetries Max number of retries to attempt?
  * @param dbUrl The database url
  * @param dbUsername The database username
  * @param dbPassword The database password
  * @param database The database name
  * @param autoParallelize Auto-parallelize big copies
  * @param maxParallelWriters Max number of concurrent writers
  * @param bytesPerPartition Max number of bytes per parallel worker
  * @param minSplitableTableSizeInBytes Don't split files with fewer than this number of bytes
  * @param whitelist A whitelist of tables to copy
  * @param blacklist A blacklist of tables to not copy
  * @param attemptIncrementalUpdates Always false for now
  */
case class ImportConfig(
  projectId: String,
  tempBucketId: String,
  datasetId: String,
  credentialsFilename: String,
  outputTablePrefix: String,
  overwriteExisting: Boolean = OverwriteExisting,
  maxRetries: Int = MaxRetries,
  dbUrl: String,
  dbUsername: String,
  dbPassword: String,
  database: String,
  autoParallelize: Boolean = AutoParallelize,
  maxParallelWriters: Int = MaxParallelWriters,
  bytesPerPartition: Long = BytesPerPartition,
  minSplitableTableSizeInBytes: Long = MinSplitableTableSizeInBytes,
  whitelist: Set[String] = Set.empty[String],
  blacklist: Set[String] = Set.empty[String],
  attemptIncrementalUpdates: Boolean = AttemptIncrementalupdates
)

object ImportConfig {
  private val OverwriteExisting = true
  private val MaxParallelWriters = 8
  private val AttemptIncrementalupdates = false
  private val MaxRetries = 1
  private val AutoParallelize = true
  private val BytesPerPartition = 100 * 1024 * 1024
  private val MinSplitableTableSizeInBytes = BytesPerPartition * 2

  implicit class RichConfig(val underlying: Config) extends AnyVal {
    def getBoolean(path: String, default: Boolean): Boolean = {
      if (underlying.hasPath(path)) {
        underlying.getBoolean(path)
      } else {
        default
      }
    }

    def getInt(path: String, default: Int): Int = {
      if (underlying.hasPath(path)) {
        underlying.getInt(path)
      } else {
        default
      }
    }
  }

  def apply(config: Config): ImportConfig = {
    val gcpConfig = config.getConfig("gcp")

    val gcpProjectID = gcpConfig.getString("project-id")
    val gcpTempBucketID = gcpConfig.getString("tempbucket-id")
    val gcpDatasetID = gcpConfig.getString("dataset-id")
    val gcpOverwriteExisting = gcpConfig.getBoolean("overwrite-existing", OverwriteExisting)
    val maxParallelWriters = config.getInt("max-parallel-writers", MaxParallelWriters)
    val gcpTablePrefix = gcpConfig.getString("table-prefix")
    val gcpJsonCredentialsFilename = System.getenv().get("GOOGLE_APPLICATION_CREDENTIALS")
    val maxRetries = config.getInt("max-retries", MaxRetries)

    val dbConf = config.getConfig("source-db")
    val url = dbConf.getString("url")
    val username = dbConf.getString("user")
    val password = dbConf.getString("password")
    val database = dbConf.getString("database")
    val autoParallelize = dbConf.getBoolean("auto-parallelize")
    val bytesPerPartition = dbConf.getLong("megabytes-per-partition") * 1024 * 1024

    val whitelist = dbConf.getStringList("tables-whitelist").asScala.map { _.toLowerCase }.toSet
    val blacklist = dbConf.getStringList("tables-blacklist").asScala.map { _.toLowerCase }.toSet

    new ImportConfig(
      projectId = gcpProjectID,
      tempBucketId = gcpTempBucketID,
      datasetId = gcpDatasetID,
      outputTablePrefix = gcpTablePrefix,
      credentialsFilename = gcpJsonCredentialsFilename,
      overwriteExisting = gcpOverwriteExisting,
      maxRetries = maxRetries,
      dbUrl = url,
      dbUsername = username,
      dbPassword = password,
      database = database,
      autoParallelize = autoParallelize,
      maxParallelWriters = maxParallelWriters,
      whitelist = whitelist,
      blacklist = blacklist
    )
  }
}
