name         := "RDFSummary"
version      := "1.0"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.0.1",
    "org.apache.spark" %% "spark-sql" % "2.0.0",
    "org.apache.jena" % "jena-arq" % "3.1.0"
)

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

scalacOptions += "-deprecation"