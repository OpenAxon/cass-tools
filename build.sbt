name := "cass-ops-agent"

lazy val root = (project in file("."))
  .aggregate(`cass-ops-agent`)
  .settings(publishArtifact := false)

// cass-ops-agent 
lazy val `cass-ops-agent` = project
  .settings(publishArtifact := false)
  .configs(IntegrationTest)

fork := true
