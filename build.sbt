name := "omopDistance"

version := "0.1"

scalaVersion := "2.10.6"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

libraryDependencies += "postgresql" % "postgresql" % "9.1-901.jdbc4"

libraryDependencies += "com.rockymadden.stringmetric" % "stringmetric-core" % "0.25.3"