ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.11.8"

lazy val root = (project in file("."))
  .settings(
    name := "student45-hw01"
  )

lazy val hadoopVersion = "3.2.1"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion
)

Compile / mainClass := Some("Student45HW01SolutionJob")
assembly / mainClass := Some("Student45HW01SolutionJob")

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}