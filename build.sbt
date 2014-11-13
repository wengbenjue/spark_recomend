import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

name := "spark_resrecomend"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.0.0-cdh5.1.2" % "provided",
  "org.apache.spark" % "spark-mllib_2.10" % "1.0.0-cdh5.1.2" % "provided",
  "com.github.scopt" % "scopt_2.10" % "3.2.0",
  "org.apache.hbase" % "hbase-client" % "0.98.1-cdh5.1.2" % "provided",
  "org.apache.hbase" % "hbase-common" % "0.98.1-cdh5.1.2" % "provided",
  "org.apache.hbase" % "hbase-hadoop-compat" % "0.98.1-cdh5.1.2" % "provided",
  "org.apache.hbase" % "hbase-it" % "0.98.1-cdh5.1.2" % "provided",
  "org.apache.hbase" % "hbase-hadoop2-compat" % "0.98.1-cdh5.1.2" % "provided",
  "org.apache.hbase" % "hbase-prefix-tree" % "0.98.1-cdh5.1.2" % "provided",
  "org.apache.hbase" % "hbase-protocol" % "0.98.1-cdh5.1.2" % "provided",
  "org.apache.hbase" % "hbase-server" % "0.98.1-cdh5.1.2" % "provided",
  "org.apache.hbase" % "hbase-shell" % "0.98.1-cdh5.1.2" % "provided",
  "org.apache.hbase" % "hbase-testing-util" % "0.98.1-cdh5.1.2" % "provided",
  "org.apache.hbase" % "hbase-thrift" % "0.98.1-cdh5.1.2" % "provided"
)

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "cloudera-repo-releases" at "https://repository.cloudera.com/artifactory/repo/",
  "central" at "http://repo1.maven.org/maven2/"
)

mainClass in assembly := Some("com.xiaomishu.com.cf.ResRecomendALS")

assemblyOption in assembly ~= { _.copy(includeScala = false) }

assemblySettings
    