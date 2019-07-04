/*
 * Copyright 2019 AstroLab Software
 * Author: Julien Peloton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import Dependencies._
import xerial.sbt.Sonatype._

resolvers += "Hortonworks Repository" at "http://repo.hortonworks.com/content/repositories/releases/"

lazy val root = (project in file(".")).
 settings(
   inThisBuild(List(
     version      := "0.2.1"
   )),
   // Name of the application
   name := "fink-broker",
   // Name of the orga
   organization := "com.github.astrolabsoftware",
   // Do not execute test in parallel
   parallelExecution in Test := false,
   // Fail the test suite if statement coverage is < 70%
   coverageFailOnMinimum := true,
   coverageMinimum := 70,
   // Put nice colors on the coverage report
   coverageHighlighting := true,
   // Do not publish artifact in test
   publishArtifact in Test := false,
   // Exclude runner class for the coverage
   // coverageExcludedPackages := "<empty>;com.astrolabsoftware.sparkfits.ReadFits*;com.astrolabsoftware.sparkfits.ReadImage*",
   // Excluding Scala library JARs that are included in the binary Scala distribution
   assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
   // Shading to avoid conflicts with pre-installed nom.tam.fits library
   // Uncomment if you have such conflicts.
   // assemblyShadeRules in assembly := Seq(ShadeRule.rename("nom.**" -> "new_nom.@1").inAll),
   // Put dependencies of the library
   libraryDependencies ++= Seq(
     "org.apache.spark" %% "spark-core" % "2.4.1" % "provided",
     "org.apache.spark" %% "spark-sql" % "2.4.1" % "provided",
     "org.apache.spark" %% "spark-streaming" % "2.4.1" % "provided",
     "com.hortonworks" % "shc-core" % "1.1.1-2.1-s_2.11",
     scalaTest % Test
   )
 )

// POM settings for Sonatype
homepage := Some(
 url("https://github.com/astrolabsoftware/fink-broker")
)
scmInfo := Some(
 ScmInfo(
   url("https://github.com/astrolabsoftware/fink-broker"),
   "https://github.com/astrolabsoftware/fink-broker.git"
 )
)

developers := List(
 Developer(
   "JulienPeloton",
   "Julien Peloton",
   "peloton@lal.in2p3.fr",
   url("https://github.com/JulienPeloton")
 )
)

licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

publishMavenStyle := true

publishTo := {
 val nexus = "https://oss.sonatype.org/"
 if (isSnapshot.value)
  Some("snapshots" at nexus + "content/repositories/snapshots")
 else
  Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}
