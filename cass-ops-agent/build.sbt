import com.twitter.scrooge.ScroogeSBT._

name := "cass-ops-agent"

version := "1.0.17"

scalaVersion := "2.10.4"

resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/maven-releases"

resolvers += "twitter" at "http://maven.twttr.com"

com.twitter.scrooge.ScroogeSBT.newSettings

// scrooge build options
scroogeBuildOptions in Compile ++= Seq("--verbose")

// generated scala code output folder
scroogeThriftOutputFolder in Compile <<= baseDirectory(_ / "src/main/scala")

libraryDependencies += "org.joda" % "joda-convert" % "1.8"

libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.7"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"

libraryDependencies += "com.typesafe" % "config" % "1.2.1"

libraryDependencies += "org.apache.thrift" % "libthrift" % "0.9.1"

libraryDependencies += "com.twitter" %% "scrooge-core" % "3.16.1"

libraryDependencies += "com.twitter" %% "finagle-core" % "6.18.0"

libraryDependencies += "com.twitter" %% "finagle-thrift" % "6.18.0"

libraryDependencies += "com.twitter" %% "util-collection" % "6.12.1"

libraryDependencies += "com.twitter" %% "util-zk" % "6.12.1"

libraryDependencies += "net.sourceforge.jtds" % "jtds" % "1.3.1"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.5"

libraryDependencies += "com.typesafe.akka" %% "akka-camel" % "2.3.5"

libraryDependencies += "org.apache.activemq" % "activemq-client" % "5.10.0"

libraryDependencies += "org.apache.activemq" % "activemq-camel" % "5.10.0"

libraryDependencies += "com.google.inject" % "guice" % "3.0"

libraryDependencies += "com.google.code.gson" % "gson" % "1.7.1"

libraryDependencies += "org.apache.cassandra" % "cassandra-all" % "2.1.0"

libraryDependencies += "org.apache.commons" % "commons-io" % "1.3.2"

libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.3.2"

libraryDependencies += "org.apache.commons" % "commons-compress" % "1.9"

libraryDependencies += "joda-time" % "joda-time" % "2.8.2"

libraryDependencies += "org.codehaus.jettison" % "jettison" % "1.3.5"

libraryDependencies += "com.indeed" % "java-dogstatsd-client" % "2.0.8"

libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.8.9.1"

libraryDependencies += "com.typesafe.slick" %% "slick" % "2.1.0"

libraryDependencies += "org.xerial" % "sqlite-jdbc" % "3.7.2"

libraryDependencies += "commons-daemon" % "commons-daemon" % "1.0.15"

libraryDependencies += "com.fasterxml.jackson.module" % "jackson-module-scala_2.10" % "2.4.2"

