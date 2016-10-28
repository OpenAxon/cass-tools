import com.twitter.scrooge.ScroogeSBT._

name := "cass-ops-agent"

resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/maven-releases"
resolvers += "twitter" at "http://maven.twttr.com"

// SBT Packager
enablePlugins(JavaAppPackaging)

// *******************************
// Cross-Compile Scala
// *******************************
val scala211 = "2.11.7"

scalaVersion in ThisBuild := scala211

crossScalaVersions in ThisBuild := Seq(scala211)

// Test
Defaults.itSettings

// Test
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % "it,test",
  "org.cassandraunit" % "cassandra-unit" % "2.1.3.1" % "test"
)

val finagleVersion = "6.25.0"
val jacksonVersion = "2.8.3"
val cassandraVersion = "2.1.15"
val cassandraDriverCore = "2.1.10.3"

// Finagle
libraryDependencies ++= Seq(
  "com.twitter" %% "finagle-core" % finagleVersion,
  "com.twitter" %% "finagle-http" % finagleVersion,
  "com.twitter" %% "finagle-thrift" % finagleVersion,
  "com.twitter" %% "finagle-stats" % finagleVersion,
  "com.twitter" %% "finagle-thriftmux" % finagleVersion,
  "com.twitter" %% "twitter-server" % "1.10.0" excludeAll(ExclusionRule(organization = "log4j"), ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12")),
  "com.twitter" %% "scrooge-core" % "3.17.0"
)

com.twitter.scrooge.ScroogeSBT.newSettings

// Core
libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.48",
  "joda-time" % "joda-time" % "2.9.4",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "ch.qos.logback" % "logback-core" % "1.1.3",  
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
  "org.apache.cassandra" % "cassandra-all" % cassandraVersion,
  "com.datastax.cassandra" % "cassandra-driver-core" % cassandraDriverCore,
  "commons-io" % "commons-io" % "2.4",
  "org.apache.commons" % "commons-lang3" % "3.4",
  "org.apache.commons" % "commons-compress" % "1.10",
  "commons-daemon" % "commons-daemon" % "1.0.15",
  "org.xerial" % "sqlite-jdbc" % "3.8.11.2",
  "com.typesafe.slick" %% "slick" % "2.1.0",
  "com.google.code.gson" % "gson" % "2.4",
  "com.indeed" % "java-dogstatsd-client" % "2.0.10"
)

libraryDependencies ~= {
  _.map(_.exclude("org.slf4j", "slf4j-jdk14"))
}
