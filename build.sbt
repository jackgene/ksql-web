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

val elmMake = taskKey[Seq[File]]("elm-make")

elmMake := {
  import com.typesafe.sbt.web.LineBasedProblem
  import play.sbt.PlayExceptions.CompilationException

  val debugFlag: String =
    if (sys.props.getOrElse("elm.debug", "false").toLowerCase != "true") ""
    else "--debug"
  var outErrLines: List[String] = Nil
  var lineNum: Option[String] = None
  var offset: Option[String] = None
  Seq(
    "bash", "-c",
    "elm-make " +
    (file("app/assets/javascripts") ** "*.elm").get.mkString(" ") +
    " --output public/javascripts/main.js " +
    s"--yes ${debugFlag} --warn"
  ).!(
    new ProcessLogger {
      override def info(s: => String): Unit = {
        streams.value.log.info(s)
        outErrLines = s :: outErrLines
      }

      override def error(s: => String): Unit = {
        streams.value.log.warn(s)
        val LineNumExtractor = """([0-9]+)\|.*""".r
        val PosExtractor = """ *\^+ *""".r
        s match {
          case LineNumExtractor(num: String) =>
            lineNum = lineNum orElse Some(num)
          case PosExtractor() =>
            offset = offset orElse Some(s)
          case _ =>
        }
        outErrLines = s :: outErrLines
      }

      override def buffer[T](f: => T): T = f
    }
  ) match {
    case 0 =>
      streams.value.log.success("elm-make completed.")
      Seq(file("public/javascripts/main.js"))

    case 127 =>
      streams.value.log.warn("elm-make not found in PATH. Skipping Elm build.")
      Nil

    case _ =>
      throw CompilationException(
        new LineBasedProblem(
          message = outErrLines.reverse.mkString("\n"),
          severity = null,
          lineNumber = lineNum.map(_.toInt).getOrElse(0),
          characterOffset = offset.map(_.indexOf('^') - 2 - lineNum.map(_.length).getOrElse(0)).getOrElse(0),
          lineContent = "",
          source = file("app/assets/javascripts/Main.elm")
        )
      )
  }
}

sourceGenerators in Assets += elmMake.taskValue
