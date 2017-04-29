package com.brigade.spark

import java.util.{Collections, UUID}

import bqutils.BigQueryServiceFactory
import com.appsflyer.spark.bigquery.BigQuerySchema
import com.google.cloud.hadoop.io.bigquery.BigQueryUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.util.Progressable
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.model._
import com.google.cloud.hadoop.io.bigquery.{BigQueryConfiguration, BigQueryOutputFormat, GsonBigQueryInputFormat}
import com.google.gson.{JsonObject, JsonParser}
import org.apache.hadoop.io.{LongWritable, NullWritable}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import com.spotify.spark.bigquery._
import org.apache.spark.sql.jdbc.JdbcDialects

import scala.util.Random

class BrigadeBigQueryUtils(spark: SparkSession, projectName: String, datasetName: String, gcsTempBucket: String, keyFile: String) extends Serializable {
  val Logger = LoggerFactory.getLogger(getClass)
  import spark.implicits._
  @transient val sqlContext = spark.sqlContext
  @transient lazy val hadoopConf = spark.sqlContext.sparkContext.hadoopConfiguration
  @transient val bigquery: Bigquery = BigQueryServiceFactory.getService


  hadoopConf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectName)
  hadoopConf.set("fs.gs.project.id", projectName)
  hadoopConf.set(BigQueryConfiguration.GCS_BUCKET_KEY, gcsTempBucket)
  hadoopConf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectName)

  sqlContext.setGcpJsonKeyFile(keyFile)

  private def waitForJob(job: Job): Unit = {
    BigQueryUtils.waitForJobCompletion(bigquery, projectName, job.getJobReference, new Progressable {
      override def progress(): Unit = {}
    })
  }

  def saveToBigquery(dataframe: DataFrame, filename: String): Unit = {
    val fqTableName = s"$projectName:$datasetName.$filename"
    dataframe.printSchema()

    dataframe
      .saveAsBigQueryTableWithRichSchema(
        fqTableName,
        WriteDisposition.WRITE_TRUNCATE,
        CreateDisposition.CREATE_IF_NEEDED
      )
  }

  def loadIntoBigTable(sourceGCSPath: String, targetTableName: String): Unit = {

    Logger.info(s"Loading from $sourceGCSPath into BigQuery table $targetTableName")
    val tableRef = new TableReference().setProjectId(projectName).setDatasetId(datasetName).setTableId(targetTableName)

    val load = new JobConfigurationLoad()
      .setDestinationTable(tableRef)
      .setAutodetect(true)
      .setWriteDisposition("WRITE_APPEND")
      .setSourceFormat("NEWLINE_DELIMITED_JSON")
      .setAllowJaggedRows(true)
      .setAllowQuotedNewlines(true)
      .setMaxBadRecords(0)
      .setSchemaUpdateOptions(List("ALLOW_FIELD_RELAXATION").asJava)
      .setSourceUris(Collections.singletonList(sourceGCSPath + "/*"))

    val job = bigquery
      .jobs()
      .insert(tableRef.getProjectId, new Job().setConfiguration(new JobConfiguration().setLoad(load)))
      .execute

    waitForJob(job)

    // Looks like the load was a success - clean up.
    delete(new Path(sourceGCSPath))
  }

  private def delete(path: Path): Unit = {
    val fs = FileSystem.get(path.toUri, hadoopConf)
    fs.delete(path, true)
  }

  private def createJobReference(projectId: String, jobIdPrefix: String): JobReference = {
    val fullJobId = projectId + "-" + UUID.randomUUID().toString
    new JobReference().setProjectId(projectId).setJobId(fullJobId)
  }

  def writeDFToGoogleStorage(inputDF: DataFrame): String = {
    hadoopConf.synchronized {
      lazy val jsonParser = new JsonParser()

      hadoopConf.set("mapreduce.job.outputformat.class", classOf[BigQueryOutputFormat[_, _]].getName)
      val bucket = hadoopConf.get(BigQueryConfiguration.GCS_BUCKET_KEY)
      val temp = s"spark-bigquery-${System.currentTimeMillis()}=${Random.nextInt(Int.MaxValue)}"
      val gcsPath = s"gs://$bucket/hadoop/tmp/spark-bigquery/$temp"

      Logger.info(s"Writing to: $gcsPath")
      hadoopConf.set(BigQueryConfiguration.TEMP_GCS_PATH_KEY, gcsPath)

      val dfColumns = inputDF.columns

      inputDF
        .toJSON
        .rdd
        .map { json =>
            val jsonParsed = jsonParser.parse(json)
            val jsonObj = jsonParsed.getAsJsonObject

            dfColumns.foreach { column =>
              if (!jsonObj.has(column)) {
                jsonObj.add(column, null)
              }
            }

          (null, jsonObj)
        }
        .saveAsNewAPIHadoopFile(hadoopConf.get(BigQueryConfiguration.TEMP_GCS_PATH_KEY),
          classOf[GsonBigQueryInputFormat],
          classOf[LongWritable],
          classOf[TextOutputFormat[NullWritable, JsonObject]],
          hadoopConf)
      gcsPath
    }
  }
}
