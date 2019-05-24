name := """Valhalla"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

resolvers += Resolver.sonatypeRepo("snapshots")

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.11.12", "2.12.6")

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"

libraryDependencies += guice
libraryDependencies += "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.2" % Test
libraryDependencies += "com.typesafe.play" %% "play-iteratees" % "2.6.1"
libraryDependencies += "com.typesafe.play" %% "play-ws" % "2.6.1"
libraryDependencies += "com.typesafe.play" %% "play-streams" % "2.6.1"
libraryDependencies += "com.typesafe.play" %% "play-iteratees-reactive-streams" % "2.6.1"
libraryDependencies += "com.h2database" % "h2" % "1.4.197"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-yarn_2.11" % "2.1.1"
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.2.3"
libraryDependencies += "com.typesafe.akka" %% "akka-slf4j" % "2.2.3"
