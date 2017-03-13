package com.brigade.spark

import java.util.{Collections, UUID}
import java.util.concurrent.ExecutionException

import bqutils.{BigQueryServiceFactory, GoogleBigQueryUtils => BQUtils}
import com.google.cloud.hadoop.io.bigquery.{BigQueryStrings, BigQueryUtils}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.util.Progressable
import org.apache.spark.sql.{DataFrame, SparkSession}
//import com.appsflyer.spark.bigquery._
import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.model._
import com.google.cloud.hadoop.io.bigquery.{BigQueryConfiguration, BigQueryOutputFormat, GsonBigQueryInputFormat}
import com.google.gson.{JsonObject, JsonParser}
import org.apache.hadoop.io.{LongWritable, NullWritable}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.slf4j.LoggerFactory

import scala.util.Random

class BrigadeBigQueryUtils(spark: SparkSession, projectName: String, datasetName: String, gcsTempBucket: String) extends Serializable {
  val Logger = LoggerFactory.getLogger(getClass)
  import spark.implicits._
  @transient val sqlContext = spark.sqlContext
  @transient lazy val hadoopConf = spark.sqlContext.sparkContext.hadoopConfiguration
  @transient val bigquery: Bigquery = BigQueryServiceFactory.getService


  hadoopConf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectName)
  hadoopConf.set("fs.gs.project.id", projectName)
  hadoopConf.set(BigQueryConfiguration.GCS_BUCKET_KEY, gcsTempBucket)
  hadoopConf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectName)

  // TODO: BigQuery claims to support compression but won't load files when this is enable.
  //       This shrinks the GCS storage by 10x so we should figure this out.
  //hadoopConf.set("mapreduce.output.fileoutputformat.compress", "true")
  //hadoopConf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec")
  //hadoopConf.set("mapreduce.output.fileoutputformat.compress.type", "RECORD")


  setGcpJsonKeyFile("gcp-prod-credentials.json")


  private def waitForJob(job: Job): Unit = {
    BigQueryUtils.waitForJobCompletion(bigquery, projectName, job.getJobReference, new Progressable {
      override def progress(): Unit = {}
    })
  }


  def setGcpJsonKeyFile(jsonKeyFile: String): Unit = {
    hadoopConf.set("mapred.bq.auth.service.account.json.keyfile", jsonKeyFile)
    hadoopConf.set("fs.gs.auth.service.account.json.keyfile", jsonKeyFile)
  }

  def loadIntoBigTable(sourceGCSPath: String, targetTableName: String): Unit = {

    Logger.info(s"Loading from $sourceGCSPath into BigQuery table $targetTableName")
    val tableRef = new TableReference().setProjectId(projectName).setDatasetId(datasetName).setTableId(targetTableName)

    val load = new JobConfigurationLoad()
      .setDestinationTable(tableRef)
      .setAutodetect(true)
      .setWriteDisposition("WRITE_TRUNCATE")
      .setSourceFormat("NEWLINE_DELIMITED_JSON")
      .setSourceUris(Collections.singletonList(sourceGCSPath + "/*"))

    val job = bigquery.jobs().insert(tableRef.getProjectId, new Job().setConfiguration(new JobConfiguration().setLoad(load))).execute

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

  def writeDFToGoogleStorage(adaptedDf: DataFrame): String = {
    lazy val jsonParser = new JsonParser()

    hadoopConf.set("mapreduce.job.outputformat.class", classOf[BigQueryOutputFormat[_, _]].getName)
    val bucket = hadoopConf.get(BigQueryConfiguration.GCS_BUCKET_KEY)
    val temp = s"spark-bigquery-${System.currentTimeMillis()}=${Random.nextInt(Int.MaxValue)}"
    val gcsPath = s"gs://$bucket/hadoop/tmp/spark-bigquery/$temp"

    Logger.info(s"Writing to: $gcsPath")
    hadoopConf.set(BigQueryConfiguration.TEMP_GCS_PATH_KEY, gcsPath)
    adaptedDf
      .toJSON
      .rdd
      .map(json => (null, jsonParser.parse(json)))
      .saveAsNewAPIHadoopFile(hadoopConf.get(BigQueryConfiguration.TEMP_GCS_PATH_KEY),
        classOf[GsonBigQueryInputFormat],
        classOf[LongWritable],
        classOf[TextOutputFormat[NullWritable, JsonObject]],
        hadoopConf)
    gcsPath
  }
}
