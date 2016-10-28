// *******************************
// Common Settings
// *******************************

organization in ThisBuild := "com.evidence"

organizationHomepage in ThisBuild := Some(url("https://www.axon.io"))

// *******************************
// Cross-Compile Scala
// *******************************
val scala211 = "2.11.7"

scalaVersion in ThisBuild := scala211

crossScalaVersions in ThisBuild := Seq(scala211)

// Nexus Repo Publishing
// *******************************
credentials in ThisBuild += Credentials(Path.userHome / ".ivy2" / ".credentials")

publishTo in ThisBuild := {
  val nexus = "https://nexus.taservs.net/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots/")
  else
    Some("releases" at nexus + "content/repositories/releases")
}
