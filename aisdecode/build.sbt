
lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "uk.gov.dft",
      scalaVersion := "2.11.8",
      version      := "0.1.0"
    )),
    name := "AisDecode",
    libraryDependencies ++= Seq(
       "org.apache.spark" %% "spark-sql" % "2.3.1",
       "org.apache.spark" %% "spark-core" % "2.3.1",
       "org.scalactic" %% "scalactic" % "3.0.5",
       "org.scalatest" %% "scalatest" % "3.0.5" % "test",
       "com.holdenkarau" %% "spark-testing-base" % "2.3.1_0.10.0" % "test",
       "org.apache.spark" %% "spark-hive" % "2.3.1" % "test"
    ),
    fork in Test := true,
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    parallelExecution in Test := false
  )


