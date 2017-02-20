package com.brigade.spark

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SQLContext}
import java.math.{BigDecimal => JBigDecimal}

import org.slf4j.LoggerFactory

case class RDBMSTable(name: String, size: Long)

class DBUtils (conf: Config, sqlContext: SQLContext) {
  import DBUtils.Logger

  val host = conf.getString("host")
  val username = conf.getString("username")
  val password = conf.getString("password")
  val database = conf.getString("database")

  val whitelist = conf.getString("tables-whitelist").split(",").map{ _.trim.toLowerCase }
  val blacklist = conf.getString("tables-blacklist").split(",").map{ _.trim.toLowerCase }

  def getConnection(tableName: String): DataFrame = {
    sqlContext
      .read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", s"jdbc:mysql://$host/$database?user=$username&password=$password")
      .option("dbtable", tableName)
      .load()
  }

  def getTablesToSync(): Seq[RDBMSTable] ={

    // TODO: probably better to just use ScalalikeJDBC; I suspect this applies the filter after fetching all tables
    val tablesDF =
      getConnection("information_schema.TABLES")
        .where(s"table_schema='$database' AND data_length is not null")

    tablesDF.collect.map { row =>

      val tableName = row.getAs[String]("table_name").toLowerCase
      val tableSize = row.getAs[JBigDecimal]("DATA_LENGTH").longValue() + row.getAs[JBigDecimal]("INDEX_LENGTH").longValue

      if (!whitelist.isEmpty && !whitelist.contains(tableName)){
        Logger.info(s"Skipping table $tableName, not on whitelist.")
      } else if (!blacklist.isEmpty && blacklist.contains(tableName)){
        Logger.info(s"Skipping table $tableName, table on blacklist.")
      }

      RDBMSTable(tableName, tableSize)
    }
  }
}

object DBUtils {
  val Logger = LoggerFactory.getLogger(getClass)
}