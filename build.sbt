import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._i

name := "spark_fruitrecomend"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.3.0-cdh5.4.7" % "provided",
  "org.apache.spark" % "spark-mllib_2.10" % "1.3.0-cdh5.4.7" % "provided",
  "com.github.scopt" % "scopt_2.10" % "3.2.0",
  "org.apache.hbase" % "hbase-client" % "1.0.0-cdh5.4.7" % "provided",
  "org.apache.hbase" % "hbase-common" % "1.0.0-cdh5.4.7" % "provided",
  "org.apache.hbase" % "hbase-hadoop-compat" % "1.0.0-cdh5.4.7" % "provided",
  "org.apache.hbase" % "hbase-it" % "1.0.0-cdh5.4.7" % "provided",
  "org.apache.hbase" % "hbase-hadoop2-compat" % "1.0.0-cdh5.4.7" % "provided",
  "org.apache.hbase" % "hbase-prefix-tree" % "1.0.0-cdh5.4.7" % "provided",
  "org.apache.hbase" % "hbase-protocol" % "1.0.0-cdh5.4.7" % "provided",
  "org.apache.hbase" % "hbase-server" % "1.0.0-cdh5.4.7" % "provided",
  "org.apache.hbase" % "hbase-shell" % "1.0.0-cdh5.4.7" % "provided",
  "org.apache.hbase" % "hbase-testing-util" % "1.0.0-cdh5.4.7" % "provided",
  "org.apache.hbase" % "hbase-thrift" % "1.0.0-cdh5.4.7" % "provided",
  "org.apache.hadoop" % "hadoop-common" % "2.6.0-cdh5.4.7"  % "provided"
)

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/rel eases/",
  "cloudera-repo-releases" at "https://repository.cloudera.com/artifactory/repo/",
  "central" at "http://repo1.maven.org/maven2/"
)

mainClass in assembly := Some("com.soledede.cf.FruitRecomendALS")

assemblyOption in assembly ~= {
  _.copy(includeScala = false)
}

assemblySettings
    