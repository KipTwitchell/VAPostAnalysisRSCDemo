
lazy val root = (project in file("."))
  .settings(
    name := "ReallySimpleCommerce",
    mainClass in (Compile, packageBin) := Some("com.ibm.VASimpleFinDemo.ReallySimpleCommerce.RSCclient")
  )

//mainClass in (Compile, run) := Some("com.ibm.univledger.universal_ledger")

exportJars := true

//libraryDependencies ++= Seq(
//)
//
////// META-INF discarding
//assemblyMergeStrategy in assembly := {
//  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//  case x => MergeStrategy.first
//}