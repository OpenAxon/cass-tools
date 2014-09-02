import AssemblyKeys._

assemblySettings

jarName in assembly := { s"${name.value}-${version.value}.jar" }

test in assembly := {}

mergeStrategy in assembly <<= (mergeStrategy in assembly) {
  (old) => {
    case PathList("org", "slf4j", "impl", xs @ _*) => MergeStrategy.last
    case PathList("javax", "xml", "stream", xs @ _*) => MergeStrategy.last
    case PathList("META-INF", xs @ _*) =>
      (xs map {_.toLowerCase}) match {
        case ("license" :: Nil) =>
          MergeStrategy.concat
        case ("license.txt" :: Nil) =>
          MergeStrategy.concat
        case ("notice" :: Nil) =>
          MergeStrategy.concat
        case ("notice.txt" :: Nil) =>
          MergeStrategy.concat
        case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
          MergeStrategy.discard
        case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") || ps.last.endsWith("pom.properties") || ps.last.endsWith("pom.xml") =>
          MergeStrategy.discard
        case "plexus" :: xs =>
          MergeStrategy.discard
        case "services" :: xs =>
          MergeStrategy.filterDistinctLines
        case ("spring.tooling" :: Nil) =>
          MergeStrategy.first
        case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
          MergeStrategy.filterDistinctLines
        case _ => MergeStrategy.deduplicate
      }
    case x => old(x)
  }
}
