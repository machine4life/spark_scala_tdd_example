name := "movielens"

version := "1.0"

scalaVersion := "2.11.8"

lazy val spark = "2.0.2"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "." + artifact.extension
}

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark,
  "org.apache.spark" %% "spark-sql" % spark,
  "org.apache.spark" %% "spark-hive" % spark % "test",
  "org.apache.spark" %% "spark-streaming" % spark,
  "log4j" % "log4j" % "1.2.14",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "2.0.2_0.4.7"
)
