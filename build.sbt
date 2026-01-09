ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.spark" %% "spark-mllib" % "3.5.0",
  "org.apache.spark" %% "spark-streaming" % "3.5.0"
)

// Resolve dependency conflicts
ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-parser-combinators" % VersionScheme.Always
)