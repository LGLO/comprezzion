scalaVersion in ThisBuild := "2.13.2"

val zioStreams = "dev.zio" %% "zio-streams" % "1.0.0-RC20"
val zioTest = "dev.zio" %% "zio-test" % "1.0.0-RC20" % Test
val zioTestSbt = "dev.zio" %% "zio-test-sbt" % "1.0.0-RC20" % Test

lazy val comprezzion =
  project
    .in(file("."))
    .settings(
      name := "comprezzion",
      fork in run := true,
      testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
      libraryDependencies := Seq(
        zioStreams,
        zioTest,
        zioTestSbt
      )
    )
