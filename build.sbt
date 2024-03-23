ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.13"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql" % "3.5.1",
  "org.scalatest" %% "scalatest" % "3.2.17" % Test,
  "com.holdenkarau" %% "spark-testing-base" % "3.5.0_1.4.7" % Test
)

lazy val root = (project in file("."))
  .settings(
    name := "project1"
  )
