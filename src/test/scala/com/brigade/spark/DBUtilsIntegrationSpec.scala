package com.brigade.spark

import com.typesafe.config.{Config, ConfigFactory}
import scalikejdbc._
import com.whisk.docker.impl.spotify.DockerKitSpotify
import com.whisk.docker.scalatest.DockerTestKit
import org.scalatest.time.{Second, Seconds, Span}
import org.scalatest.{FlatSpec, Matchers}

class DBUtilsIntegrationSpec
  extends FlatSpec
    with Matchers
    with DockerTestKit
    with DockerKitSpotify
    with DockerMysqlService {

  import scala.concurrent.duration._
  override val StartContainersTimeout = 45 seconds

  implicit val pc = PatienceConfig(Span(120, Seconds), Span(1, Second))

  val appConfig = ConfigFactory.parseString(
    s""" source-db {
      |   host = "127.0.0.1"
      |   port = "$MysqlExposedPort"
      |   username = $MysqlUser
      |   password = "$MysqlPassword"
      |   database = "$MysqlDatabase"
      |   tables-whitelist = [recipient_domains, tags]
      | }
    """.stripMargin
  ).withFallback(ConfigFactory.defaultReference())

  lazy val dbUtils = new DBUtils(appConfig.getConfig("source-db"))

  "a simple table" should "have a discoverable primary key" in {
    dbUtils.getConnection().autoCommit { implicit session =>

      sql"""create table mytable(keycolumn INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY)
            """.execute().apply()

      val columns = dbUtils.getKeyColumn("mytable")
      columns should contain("keycolumn")
    }
  }

  "a simple table with no primary key" should "return no key column" in {
    dbUtils.getConnection().autoCommit { implicit session =>

      sql"""create table mytable(mycolumn INT(11) NOT NULL)
          """.execute().apply()

      val columns = dbUtils.getKeyColumn("mytable")
      columns.size should be (0)
    }
  }
}
