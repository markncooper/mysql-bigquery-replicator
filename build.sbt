/*
 * Copyright 2016 Brigade Media.
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
import sbtassembly.AssemblyPlugin.autoImport.{ShadeRule, _}

name := "mysql-bigquery-replicator"
organization := "com.brigade"
scalaVersion := "2.11.8"
crossScalaVersions := Seq("2.10.6", "2.11.8")

spName := "brigade/mysql-bigquery-replicator"
sparkVersion := "2.0.2"
sparkComponents := Seq("core", "sql")
spAppendScalaVersion := false
spIncludeMaven := true
spIgnoreProvided := true
parallelExecution in Test := false
publishMavenStyle := true
isSnapshot := true

libraryDependencies ++= Seq(
//  "com.databricks" %% "spark-avro" % "3.2.0",
  "com.google.guava" % "guava" % "20.0",
  "com.google.apis" % "google-api-services-bigquery" % "v2-rev336-1.22.0",
  "com.google.cloud" % "google-cloud-bigquery" % "0.9.3-beta",
  "com.google.cloud.bigdataoss" % "bigquery-connector" % "0.10.1-hadoop2"
    exclude ("com.google.guava", "guava-jdk5"),

  "com.spotify" %% "spark-bigquery" % "0.2.1-BRIGADE02",
  "com.typesafe" % "config" % "1.2.1",
  "mysql" % "mysql-connector-java" % "5.1.36",
  "org.scalikejdbc" %% "scalikejdbc" % "2.5.0",
  "org.scalikejdbc" %% "scalikejdbc-test" % "2.5.0",

  "org.apache.httpcomponents" % "httpclient" % "4.5.3",

  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.8.6",

  "com.whisk" %% "docker-testkit-scalatest" % "0.9.0" % "test",
  "com.whisk" %% "docker-testkit-impl-spotify" % "0.9.0" % "test"
)

resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
resolvers += "Nexus" at "http://nexus.brigade.zone:8081/nexus/content/repositories/releases"

// Release settings
licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")
releaseCrossBuild             := true
releasePublishArtifactsAction := PgpKeys.publishSigned.value
pomExtra                      := {
  <url>https://github.com/brigade/mysql-bigquery-replicator</url>
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


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.last
}

assemblyShadeRules in assembly := Seq(
  // Conflicts with Spark
  ShadeRule.rename("com.google.common.**" -> "shade.com.google.common.@1").inAll
)

//
// Publish to our internal repo
//
lazy val nexusRepoHost = System.getProperty("NEXUS_HOST", "-")
lazy val nexusRepoUrl = System.getProperty("NEXUS_REPO_URL", "-")
lazy val nexusUser = System.getProperty("NEXUS_USER", "-")
lazy val nexusPassword = System.getProperty("NEXUS_PASSWORD", "-")

credentials += Credentials("Sonatype Nexus Repository Manager", nexusRepoHost, nexusUser, nexusPassword)
publishTo := Some("Brigade Nexus Repo" at nexusRepoUrl)
