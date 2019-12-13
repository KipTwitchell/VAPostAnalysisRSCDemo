
lazy val root = (project in file("."))
  .settings(
    name := "VAAnalysisDemo",
    mainClass in (Compile, packageBin) := Some("com.ibm.VASimpleFinDemo.VAAnalysisDemo.instAnalysis")
  )

//mainClass in (Compile, run) := Some("com.ibm.univledger.universal_ledger")

exportJars := true
//
//libraryDependencies ++= Seq(
//)
//
////// META-INF discarding
//assemblyMergeStrategy in assembly := {
//  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//  case x => MergeStrategy.first
//}