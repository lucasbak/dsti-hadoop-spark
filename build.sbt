

name := "spark2-to-elastic"

version := "1.0"

scalaVersion := "2.11.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0" % "provided"
// https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-spark-20
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "5.6.7"

resolvers += "conjars.org" at "http://conjars.org/repo"

lazy val commonSettings = Seq(
  version := "0.1-spark2-to-elastic",
  organization := "com.adaltas.bakalian",
  scalaVersion := "2.11.4"
)

lazy val app = (project in file("app")).
  settings(commonSettings: _*).
  settings(
    assemblyJarName in assembly := "spark2-to-elastic-fat.jar",
    mainClass in assembly := Some("Transformer")
  )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}


/*artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.copy(`classifier` = Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case PathList("com", xs @ _*)                              => MergeStrategy.last
  case PathList("org", xs @ _*)                              => MergeStrategy.last
  case "plugin.xml"                              => MergeStrategy.last
  case "parquet.thrift"                              => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
*/
test in assembly := {}