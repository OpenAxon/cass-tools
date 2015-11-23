import com.twitter.scrooge.ScroogeSBT._

name := "cass-ops-agent"

version := "1.1.0"

resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/maven-releases"
resolvers += "twitter" at "http://maven.twttr.com"

// SBT Packager
enablePlugins(JavaAppPackaging)

// Test
Defaults.itSettings

// Test
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % "it,test",
  "org.cassandraunit" % "cassandra-unit" % "2.1.3.1" % "test"
)

val finagleVersion = "6.25.0"
val jacksonVersion = "2.5.2"
val cassandraVersion = "2.1.0"

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
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.10",
  "org.slf4j" % "jul-to-slf4j" % "1.7.10",
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
  "org.apache.cassandra" % "cassandra-all" % cassandraVersion,
  "com.datastax.cassandra" % "cassandra-driver-core" % cassandraVersion,
  "commons-io" % "commons-io" % "2.4",
  "org.apache.commons" % "commons-lang3" % "3.4",
  "org.apache.commons" % "commons-compress" % "1.10",
  "commons-daemon" % "commons-daemon" % "1.0.15",
  "com.amazonaws" % "aws-java-sdk" % "1.10.34",
  "org.xerial" % "sqlite-jdbc" % "3.8.11.2",
  "com.typesafe.slick" %% "slick" % "2.1.0",
  "com.google.code.gson" % "gson" % "2.4",
  "com.indeed" % "java-dogstatsd-client" % "2.0.10"
)

libraryDependencies ~= {
  _.map(_.exclude("org.slf4j", "slf4j-jdk14"))
}