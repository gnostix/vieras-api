import sbt._
import Keys._
import org.scalatra.sbt._
import org.scalatra.sbt.PluginKeys._
import com.mojolly.scalate.ScalatePlugin._
import ScalateKeys._

object ScalatraoraBuild extends Build {
  val Organization = "gr.gnostix"
  val Name = "ScalatraOra"
  val Version = "0.1.0-SNAPSHOT"
  val ScalaVersion = "2.10.3"
  val ScalatraVersion = "2.2.2"

  lazy val project = Project (
    "scalatraora",
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
        "org.scalatra" %% "scalatra" % ScalatraVersion,
        "org.scalatra" %% "scalatra-scalate" % ScalatraVersion,
        "org.scalatra" %% "scalatra-specs2" % ScalatraVersion % "test",
        "ch.qos.logback" % "logback-classic" % "1.0.6" % "runtime",
        "org.eclipse.jetty" % "jetty-webapp" % "8.1.8.v20121106" % "container",
        "org.eclipse.jetty.orbit" % "javax.servlet" % "3.0.0.v201112011016" % "container;provided;test" artifacts (Artifact("javax.servlet", "jar", "jar")),
        "com.typesafe.slick" %% "slick" % "2.0.1",
        "org.slf4j" % "slf4j-nop" % "1.6.4",
        "c3p0" % "c3p0" % "0.9.1.2"
      ),

  // Oracle deps
         libraryDependencies += "com.typesafe.slick" %% "slick-extensions" % "2.0.1",
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
