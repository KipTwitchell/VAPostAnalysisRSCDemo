
//lazy val root = (project in file("."))
//  .settings(
//    name := "VAMatchMerge",
//    mainClass in (Compile, packageBin) := Some("com.ibm.VASimpleFinDemo.VAMatchMerge.JoinTest")
//  )

name := "VAMatchMerge"

mainClass in (Compile, packageBin) := Some("VAMatchMerge.JoinTest")

//mainClass in (Compile, run) := Some("com.ibm.univledger.universal_ledger")

exportJars := true

//libraryDependencies +=


//
////// META-INF discarding
//assemblyMergeStrategy in assembly := {
//  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//  case x => MergeStrategy.first
//}