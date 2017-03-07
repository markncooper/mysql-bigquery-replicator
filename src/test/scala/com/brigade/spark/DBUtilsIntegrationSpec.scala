package com.brigade.spark

import com.typesafe.config.{Config, ConfigFactory}
import scalikejdbc._
import scalikejdbc.scalatest.AutoRollback
import com.whisk.docker.impl.spotify.DockerKitSpotify
import com.whisk.docker.scalatest.DockerTestKit
import org.scalatest.Matchers
import org.scalatest.time.{Second, Seconds, Span}
import org.scalatest.fixture.FlatSpec

class DBUtilsIntegrationSpec
  extends FlatSpec
    with DockerTestKit
    with DockerKitSpotify
    with DockerMysqlService
    with AutoRollback
    with Matchers {

  import scala.concurrent.duration._
  override val StartContainersTimeout = 45 seconds

  implicit val pc = PatienceConfig(Span(120, Seconds), Span(1, Second))

  val appConfig = ConfigFactory.parseString(
    s""" source-db {
      |   url = "jdbc:mysql://127.0.0.1:$MysqlExposedPort/$MysqlDatabase"
      |   user = $MysqlUser
      |   password = "$MysqlPassword"
      |   database = "$MysqlDatabase"
      |   tables-whitelist = [recipient_domains, tags]
      | }
    """.stripMargin
  ).withFallback(ConfigFactory.defaultReference())

  lazy val dbUtils = new DBUtils(appConfig.getConfig("source-db"))

  override def db = dbUtils.getConnection().toDB

  it should "should find the primary key" in { implicit session =>
    sql"""create table mytable1(keycolumn INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY)
          """.execute().apply()

    val columns = dbUtils.getKeyColumn("mytable1")
    columns should contain("keycolumn")
  }

  it should "not find a primary key when none exists" in { implicit session =>
    sql"""create table mytable2(mycolumn INT(11) NOT NULL)
            """.execute().apply()
    val columns = dbUtils.getKeyColumn("mytable2")
    columns.size should be (0)
  }
}
