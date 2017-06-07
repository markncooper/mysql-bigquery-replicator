package com.brigade.spark

import org.apache.spark.sql.{DataFrame, SQLContext}
import java.util.TimeZone

import org.slf4j.LoggerFactory
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings, NamedDB}
import scalikejdbc._

case class RDBMSTable(name: String, size: Long, immutable: Boolean, keyColumnName: Option[String], minKey: Option[Long], maxKey: Option[Long]){
  def getKeyColumnIsKnown(): Boolean ={
    keyColumnName.isDefined
  }
}

class DBUtils (conf: ImportConfig) {
  import DBUtils.Logger

  private val connProps = new java.util.Properties()
  connProps.put("driver", "com.mysql.jdbc.Driver")
  connProps.put("user", conf.dbUsername)
  connProps.put("password", conf.dbPassword)

  Class.forName("com.mysql.jdbc.Driver")

  val connectionPoolSettings = ConnectionPoolSettings(
    initialSize = 1,
    maxSize = 5,
    connectionTimeoutMillis = 3000L,
    validationQuery = "select 1 from dual")

  setupConnectionPool(conf.database)

  def setupConnectionPool(poolName: String): ConnectionPool = {
    if (!ConnectionPool.isInitialized(poolName)) {

      // make sure jdbc is using the right timezone
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
      ConnectionPool.add (
        poolName,
        conf.dbUrl,
        conf.dbUsername,
        conf.dbPassword,
        connectionPoolSettings
      )
    }
    ConnectionPool.get(poolName)
  }

  def getConnection() = {
    NamedDB(conf.database)
  }

  def getKeyColumn(tableName: String): List[String] = {
    NamedDB(conf.database) readOnly { implicit session =>
      sql"""
         SELECT k.column_name
         FROM information_schema.table_constraints t
         JOIN information_schema.key_column_usage k
         USING(constraint_name,table_schema,table_name)
         WHERE t.constraint_type='PRIMARY KEY'
           AND t.table_schema=${conf.database}
           AND t.table_name=$tableName
         """
        .map (rs => rs.string("column_name"))
        .list()
        .apply()
    }
  }

  def getMaxValue(tableName: String, columnName: String): Option[Long] = {
    NamedDB(conf.database) readOnly { implicit session =>
      val tableNoQuotes = SQLSyntax.createUnsafely(tableName)
      val idFieldNoQuotes = SQLSyntax.createUnsafely(columnName)

      sql"SELECT max($idFieldNoQuotes) as max_val from $tableNoQuotes"
        .map { rs =>
          rs.long("max_val")
        }
        .single()
        .apply()
    }
  }

  def getColumnNames(tableName: String): List[String] = {
    NamedDB(conf.database) readOnly { implicit session =>
      sql"""SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA=${conf.database}
      AND TABLE_NAME=$tableName"""
        .map(rs => rs.string("COLUMN_NAME"))
        .list()
        .apply()
    }
  }

  def getSourceDF(table: RDBMSTable)(implicit sqlContext: SQLContext): DataFrame = {
    if (conf.autoParallelize && table.getKeyColumnIsKnown()){
      val min = table.minKey.get
      val max = table.maxKey.get
      val sizeOfSync = table.size.toFloat * ( (max.toFloat - min.toFloat) / max.toFloat)

      val partitions = math.ceil(sizeOfSync / conf.bytesPerPartition).toInt

      Logger.info(s"Build an DF for table ${table.name} with $min / $max / $partitions")
      sqlContext
        .read
        .jdbc(
          conf.dbUrl,
          s"${conf.database}.${table.name}",
          table.keyColumnName.get,
          min,
          max,
          partitions,
          connProps
        )
    } else {
      Logger.info(s"Build an DF for table ${table.name} with NO parallelization.")

      sqlContext
        .read
        .jdbc(conf.dbUrl, s"${conf.database}.${table.name}", connProps)
    }
  }

  def getTablesToSync()(implicit sqlContext: SQLContext): Seq[RDBMSTable] ={
    val tableNameSize =
      NamedDB(conf.database) readOnly { implicit session =>
        sql"""SELECT table_name, table_rows, data_length, index_length
              FROM information_schema.TABLES
              WHERE table_schema=${conf.database} AND data_length IS NOT null"""
          .map { rs =>
            val tableName = rs.string("table_name")
            val tableRows = rs.long("table_rows")
            val tableSize = rs.long("data_length") + rs.long("index_length")
            (tableName, tableRows, tableSize)
          }
          .list()
          .apply()
      }

    val filteredTableSize =
      tableNameSize.flatMap { case (tableName, tableRows, tableSize) =>
        if (tableRows == 0){
          Logger.info(s"Skipping table $tableName, table has no rows.")
          None
        } else if (!conf.whitelist.isEmpty && !conf.whitelist.contains(tableName)){
          Logger.info(s"Skipping table $tableName, not on whitelist.")
          None
        } else if (!conf.blacklist.isEmpty && conf.blacklist.contains(tableName)){
          Logger.info(s"Skipping table $tableName, table on blacklist.")
          None
        } else {
          Some(tableName, tableSize)
        }
      }

    filteredTableSize.map { case (name, size) =>
      Logger.info(s"Finding keys for $name")
      val columns = getColumnNames(name)
      val mutable = columns.contains("updated_at")
      val keyColumn = getKeyColumn(name)

      if (size <= conf.minSplitableTableSizeInBytes){
        Logger.warn(s"Skipping key discovered for small table $name")
        RDBMSTable(name, size, !mutable, None, None, None)
      } else if (keyColumn.nonEmpty) {

        // TODO: for tables with composite keys we need a better approach
        if (keyColumn.size > 1) {
          Logger.warn(s"Table $name has a composite key column. Using only ${keyColumn.head}")
        }

        try {
          val maxKeyValue = getMaxValue(name, keyColumn.head)
          RDBMSTable(name, size, !mutable, keyColumn.headOption, Option(0), maxKeyValue)
        } catch {
          case _: ResultSetExtractorException => {
            Logger.warn(s"Couldn't find primary key or primary key not a number for table $name. ")
            RDBMSTable(name, size, !mutable, None, None, None)
          }
        }
      } else {
        Logger.warn(s"Couldn't find primary key for table $name. ")
        RDBMSTable(name, size, !mutable, None, None, None)
      }
    }
  }
}

object DBUtils {
  val Logger = LoggerFactory.getLogger(getClass)
}