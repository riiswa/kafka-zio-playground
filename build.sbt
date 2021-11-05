name := "kafka-zio-playground"

version := "0.1"

scalaVersion := "3.0.2"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.0.0"

libraryDependencies ++= Seq(
  "dev.zio" %% "zio" % "2.0.0-M4",
  "dev.zio" %% "zio-streams" % "2.0.0-M4",
  "dev.zio" %% "zio-test" % "2.0.0-M4" % "test",
  "dev.zio" %% "zio-test-sbt" % "2.0.0-M4" % "test"
)


testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
