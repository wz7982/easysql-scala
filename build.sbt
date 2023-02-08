name := "easysql-scala"

lazy val commonSettings = Seq(
    organization := "easysql",
    version := "1.0.0",
    scalaVersion := "3.2.1"
)

lazy val core = project.in(file("core")).settings(commonSettings)
lazy val jdbc = project.in(file("jdbc")).dependsOn(core).settings(commonSettings)