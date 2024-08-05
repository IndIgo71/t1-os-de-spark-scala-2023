ThisBuild / version := "1.0.0"

ThisBuild / scalaVersion := "2.11.8"

lazy val root = (project in file("."))
  .settings(
    name := "student45-hw03"
  )

lazy val sparkVersion = "2.3.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

Compile / mainClass := Some("student52_hw3_task02")
assembly / mainClass := Some("student52_hw3_task02")

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
