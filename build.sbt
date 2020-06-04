name := "magcb"

version := "0.1"

scalaVersion := "2.13.2"

lazy val commonSettings = Seq(

  scalaVersion := "2.13.2"
)

//lazy val flowAlgebra = project.settings(
//  commonSettings
//)
lazy val apps = project.dependsOn(core).settings(
  commonSettings,
  scalacOptions ++= Seq("-Ymacro-annotations"),
  libraryDependencies ++= Seq(
    "io.circe" %% "circe-core",
    "io.circe" %% "circe-generic",
    "io.circe" %% "circe-parser"
  ).map(_ % "0.12.3")
)
lazy val core = project.dependsOn().settings(
  commonSettings,
  scalacOptions ++= Seq("-Ymacro-annotations"),
  libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "org.scala-graph" %% "graph-core" % "1.13.2",
    "com.github.ajrnz" %% "scemplate" % "0.5.2"
  )
)