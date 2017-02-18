/*
 * Copyright 2016 Appsflyer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

name := "mysql-bigquery-replicator"
organization := "com.brigade"
scalaVersion := "2.11.8"
crossScalaVersions := Seq("2.10.6", "2.11.8")

spName := "appsflyer-dev/spark-bigquery"
sparkVersion := "2.0.0"
sparkComponents := Seq("core", "sql")
spAppendScalaVersion := false
spIncludeMaven := true
spIgnoreProvided := true
credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")
parallelExecution in Test := false
libraryDependencies ++= Seq(
  "com.appsflyer" %% "spark-bigquery" % "0.1.1",
  "org.mockito" % "mockito-core" % "1.8.5" % "test",
  "org.scalatest" %% "scalatest" % "2.2.5" % "test"
)

// Release settings
licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")
releaseCrossBuild             := true
releasePublishArtifactsAction := PgpKeys.publishSigned.value
pomExtra                      := {
  <url>https://github.com/appsflyer-dev/spark-bigquery</url>
  <scm>
    <url>git@github.com/markncooper/mysql-bigquery-replicator.git</url>
    <connection>scm:git:git@github.com:markncooper/mysql-bigquery-replicator.git</connection>
  </scm>
  <developers>
    <developer>
      <id>markncooper</id>
      <name>Mark Cooper</name>
      <email>mark.cooper@brigade.com</email>
      <url>https://github.com/markncooper</url>
    </developer>
  </developers>
}