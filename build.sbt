val sparkVersion = "2.1.1"

name := "Simple Project"

version := "1.0"

scalaVersion := "2.11.7"
test in assembly := {} //To skip the test during assembly,


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"

)

assemblyJarName in assembly := name.value + ".jar"

lazy val root = (project in file("."))
