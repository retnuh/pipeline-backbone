name := """pipeline-backbone"""
organization := "ie.zalando.dougal"
name := "pipeline-backbone"
description := "An abstraction for data-extraction pipelines"
version := "git describe --tags --dirty --always".!!.stripPrefix("v").trim

scalaVersion := "2.11.8"
crossScalaVersions := Seq("2.11.7", "2.10.6")
lazy val kafkaVersion = "0.10.2.0"

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats" % "0.7.2"

  , "org.apache.kafka" %% "kafka" % kafkaVersion % Provided
  , "org.apache.kafka" % "kafka-streams" % kafkaVersion % Provided

  , "org.scalatest" %% "scalatest" % "2.2.4" % Test
  , "net.manub" %% "scalatest-embedded-kafka" % "0.12.0" % Test
  , "net.manub" %% "scalatest-embedded-kafka-streams" % "0.12.0" % Test
)

addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.0")

ivyScala := ivyScala.value map {
  _.copy(overrideScalaVersion = true)
}

evictionWarningOptions in update := EvictionWarningOptions.default
  .withWarnTransitiveEvictions(false)
  .withWarnDirectEvictions(false)
  .withWarnScalaVersionEviction(false)

publishMavenStyle := true
bintrayOrganization := Some("fashioninsightscentre")
bintrayOmitLicense := true
bintrayRepository := "releases"

scalariformSettings
jacoco.settings
