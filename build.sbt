import Dependencies._

ThisBuild / scalaVersion     := "2.11.12"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "boston-crimes-map",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.6" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.6" % "provided"
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
