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
libraryDependencies +=  "org.webjars" % "codemirror" % "5.33.0"
libraryDependencies += "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.2" % Test

watchSources := watchSources.value.filter { _.getName != "main.js" }

// Adds additional packages into Twirl
//TwirlKeys.templateImports += "com.connexity.plm.controllers._"

// Adds additional packages into conf/routes
// play.sbt.routes.RoutesKeys.routesImport += "com.connexity.plm.binders._"

lazy val elmMake = taskKey[Seq[File]]("elm-make")

elmMake := {
  val debugFlag: String =
    if (sys.props.getOrElse("elm.debug", "false").toLowerCase != "true") ""
    else "--debug"
  Seq(
    "bash", "-c",
    "elm-make app/assets/javascripts/Main.elm " +
    "--output public/javascripts/main.js " +
    s"--yes ${debugFlag} --warn"
  ).! match {
    case 0 =>
      streams.value.log.success("elm-make completed.")
      Seq(new File("public/javascripts/main.js"))

    case 127 =>
      streams.value.log.warn("elm-make not found in PATH. Skipping Elm build.")
      Nil

    case status =>
      throw new IllegalArgumentException(
        s"elm-make failed with non-zero exit ${status}"
      )
  }
}

sourceGenerators in Assets += elmMake.taskValue
