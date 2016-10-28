val buildVersion = "1.2.0-SNAPSHOT"

// *******************************
// Semantic Versioning Update
// *******************************
val VersionRegex = """([0-9]+)(?:(?:\.([0-9]+))?(?:\.([0-9]+))?)?([\-0-9a-zA-Z]*)?""".r

// We need to remove whitespace here for 0.13.5 sbt, but 0.13.7 sbt includes them just fine
def setVersionFunc(versionStr: String): String = {
  val VersionRegex(major, minor, defaultBld, defaultQual) = versionStr

  val qualOverride = System.getProperty("packaging.buildQualifier")

  val qual = if (qualOverride == null) {
    defaultQual
  } else {
    qualOverride
  }

  // If this is string.Empty it should explode in a fiery battle.
  val bldOverride = System.getProperty("packaging.buildNumber")

  val bld = if (bldOverride == null) {
    defaultBld
  } else if (bldOverride.isEmpty()) {
    throw new Exception("packaging.buildNumber has to be set if it is used")
  } else {
    bldOverride
  }

  s"$major.$minor.$bld$qual"
}

// Avoid weird cyclical dependencies in SBT
version := setVersionFunc(buildVersion)

version in ThisBuild := setVersionFunc(buildVersion)

// *******************************
// Output version to target
// *******************************
lazy val outputVersion = taskKey[Unit]("Outputs current version to version.txt in targets")

outputVersion in Compile := {
  val file = baseDirectory.value / "target" / "version.txt"
  IO.write(file, version in ThisBuild value)
}