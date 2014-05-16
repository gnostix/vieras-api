import sbt._
import Keys._
import org.scalatra.sbt._
import org.scalatra.sbt.PluginKeys._
import com.mojolly.scalate.ScalatePlugin._
import ScalateKeys._

object ScalatraoraBuild extends Build {
  val Organization = "gr.gnostix"
  val Name = "GnostixAPI"
  val Version = "0.1.0"
  val ScalaVersion = "2.10.3"
  val ScalatraVersion = "2.2.2"

  lazy val project = Project (
    "GnostixAPI",
    file("."),
    settings = Defaults.defaultSettings ++ ScalatraPlugin.scalatraWithJRebel ++ scalateSettings ++ Seq(
      organization := Organization,
      name := Name,
      version := Version,
      scalaVersion := ScalaVersion,
      resolvers += Classpaths.typesafeReleases,
      resolvers += "Sonatype Releases"  at "http://oss.sonatype.org/content/repositories/releases",
      resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/maven-releases/",
      libraryDependencies ++= Seq(
        "org.scalatra" %% "scalatra" % ScalatraVersion withJavadoc(),
        "org.scalatra" %% "scalatra-scalate" % ScalatraVersion withJavadoc(),
        "org.scalatra" %% "scalatra-specs2" % ScalatraVersion % "test" withJavadoc(),
        "ch.qos.logback" % "logback-classic" % "1.0.6" % "runtime" withJavadoc(),
        "org.eclipse.jetty" % "jetty-webapp" % "8.1.8.v20121106" % "container" withJavadoc(),
        "org.eclipse.jetty.orbit" % "javax.servlet" % "3.0.0.v201112011016" % "container;provided;test" artifacts (Artifact("javax.servlet", "jar", "jar")),
        "c3p0" % "c3p0" % "0.9.1.2" withJavadoc(),
				"org.scalatra" %% "scalatra-json" % "2.2.2" withJavadoc(),
				"org.json4s"   %% "json4s-jackson" % "3.2.6" withJavadoc(),
        "com.typesafe.slick" %% "slick" % "2.0.2" withJavadoc(),
        "com.typesafe.slick" %% "slick-extensions" % "2.0.2" withJavadoc(),
        "org.scalatra" %% "scalatra-auth" % ScalatraVersion withJavadoc()
      ),

      unmanagedJars in Compile ++= {
        val base = baseDirectory.value
        val customJars = (base / "custom_lib" ** "*.jar")
        customJars.classpath
      },

      scalateTemplateConfig in Compile <<= (sourceDirectory in Compile){ base =>
        Seq(
          TemplateConfig(
            base / "webapp" / "WEB-INF" / "templates",
            Seq.empty,  /* default imports should be added here */
            Seq(
              Binding("context", "_root_.org.scalatra.scalate.ScalatraRenderContext", importMembers = true, isImplicit = true)
            ),  /* add extra bindings here */
            Some("templates")
          )
        )
      }
    )
  )
}
