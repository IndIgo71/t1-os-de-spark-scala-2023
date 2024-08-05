ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.11.8"

lazy val root = (project in file("."))
  .settings(
    name := "student45-hw07"
  )

lazy val sparkVersion = "2.3.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "net.datafaker" % "datafaker" % "1.8.1"
)

Compile / mainClass := Some("student45_hw07")
assembly / mainClass := Some("student45_hw07")



ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "ivanovna_hw05" => MergeStrategy.discard
  case x => MergeStrategy.first
}