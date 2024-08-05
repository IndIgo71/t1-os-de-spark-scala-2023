ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.11.8"

lazy val root = (project in file("."))
  .settings(
    name := "student45-hw05"
  )

lazy val sparkVersion = "2.3.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.postgresql" % "postgresql" % "42.6.0"
)

Compile / mainClass := Some("student45_hw05")
assembly / mainClass := Some("student45_hw05")

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}