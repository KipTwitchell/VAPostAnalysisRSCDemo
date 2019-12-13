import sbt.Keys.licenses

lazy val commonSettings = Seq(
  organization := "com.ibm.VASimpleFinDemo",

  version := "0.1.0-SNAPSHOT",

  scalaVersion := "2.11.8"

)

//name := "VASimpleFinDemo"

lazy val root = Project(id="VASimpleFinDemo", base = file("."))

lazy val VAPostingDemo = (project in file("VAPostingDemo"))
  .settings(
    commonSettings,
    // other settings
  )

lazy val VAAnalyisDemo = (project in file("VAAnalysisDemo"))
  .settings(
    commonSettings,
    // other settings
  )


lazy val ReallySimpleCommerce = (project in file("ReallySimpleCommerce"))
  .settings(
    commonSettings,
    // other settings
  )

lazy val VAMatchMerge = (project in file("VAMatchMerge"))
  .settings(
    commonSettings,
    // other settings
  )
