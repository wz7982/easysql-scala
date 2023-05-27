name := "easysql-scala"

lazy val commonSettings = Seq(
    organization := "easysql",
    version := "1.0.0",
    scalaVersion := "3.3.0",
    scalacOptions += "-Yexplicit-nulls",
    libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "2.3.0"
)

lazy val core = project.in(file("core")).settings(commonSettings)
lazy val jdbc = project.in(file("jdbc")).dependsOn(core).settings(commonSettings)
