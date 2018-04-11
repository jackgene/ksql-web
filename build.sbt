name := """ksql-web"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.12.4"

scalacOptions ++= Seq("-feature", "-deprecation")

resolvers += Resolver.mavenLocal
resolvers += "Maven central mirror" at "http://nexus.shopzilla.com:2099/nexus/content/repositories/central"
resolvers += "Internal releases" at "http://nexus.shopzilla.com:2099/nexus/content/repositories/internal-releases"
resolvers += "Internal snapshots" at "http://nexus.shopzilla.com:2099/nexus/content/repositories/internal-snapshots"
resolvers += "Internal 3rd party" at "http://nexus.shopzilla.com:2099/nexus/content/repositories/thirdparty"

externalResolvers := Resolver.withDefaultResolvers(resolvers.value, mavenCentral = false)

libraryDependencies += guice
libraryDependencies += ws
libraryDependencies += "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.2" % Test

// Adds additional packages into Twirl
//TwirlKeys.templateImports += "com.connexity.plm.controllers._"

// Adds additional packages into conf/routes
// play.sbt.routes.RoutesKeys.routesImport += "com.connexity.plm.binders._"
