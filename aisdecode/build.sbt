
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
       "org.apache.spark" %% "spark-core" % "2.3.1"
    )
  )
