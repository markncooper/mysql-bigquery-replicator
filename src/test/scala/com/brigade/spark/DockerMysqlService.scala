package com.brigade.spark

import java.sql.DriverManager

import com.whisk.docker.{DockerCommandExecutor, DockerContainer, DockerContainerState, DockerKit, DockerReadyChecker}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

trait DockerMysqlService extends DockerKit {
  import scala.concurrent.duration._

  def MysqlAdvertisedPort = 3306
  def MysqlExposedPort = 33000
  val MysqlUser = "root"
  val MysqlPassword = "mysecretpassword"
  val MysqlDatabase = "db"

  val mysqlContainer = DockerContainer("mysql:5.7.14")
    .withPorts((MysqlAdvertisedPort, Some(MysqlExposedPort)))
    .withEnv(s"MYSQL_USER=$MysqlUser", s"MYSQL_ROOT_PASSWORD=$MysqlPassword", s"MYSQL_DATABASE=$MysqlDatabase")
    .withReadyChecker(
      new MysqlReadyChecker(MysqlUser, MysqlPassword).looped(120, 1.second)
    )

  abstract override def dockerContainers: List[DockerContainer] =
    mysqlContainer :: super.dockerContainers
}

class MysqlReadyChecker(user: String, password: String, port: Option[Int] = None) extends DockerReadyChecker {

  override def apply(container: DockerContainerState)(implicit docker: DockerCommandExecutor, ec: ExecutionContext): Future[Boolean] =
    container.getPorts().map(ports =>
      Try {
        Class.forName("com.mysql.jdbc.Driver")
        val url = s"jdbc:mysql://${docker.host}:${port.getOrElse(ports.values.head)}"
        Option(DriverManager.getConnection(url, user, password))
          .map(_.close)
          .isDefined
      }.getOrElse(false)
    )
}