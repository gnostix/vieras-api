import sbt._
import Keys._
import org.scalatra.sbt._
import com.mojolly.scalate.ScalatePlugin._
import ScalateKeys._


object ScalatraoraBuild extends Build {
  val Organization = "gr.gnostix"
  val Name = "GnostixAPI"
  val Version = "0.1.0"
  val ScalaVersion = "2.10.4"
  val ScalatraVersion = "2.3.0"

  lazy val project = Project(
    "GnostixAPI",
    file("."),
    settings = Defaults.defaultSettings ++ ScalatraPlugin.scalatraWithJRebel ++ scalateSettings ++ Seq(
      organization := Organization,
      name := Name,
      version := Version,
      scalaVersion := ScalaVersion,
      resolvers += Classpaths.typesafeReleases,
      resolvers += "Sonatype Releases" at "http://oss.sonatype.org/content/repositories/releases",
      resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/maven-releases/",
      libraryDependencies ++= Seq(
        "org.scalatra" %% "scalatra" % ScalatraVersion withJavadoc(),
        "org.scalatra" %% "scalatra-scalate" % ScalatraVersion withJavadoc(),
        "org.scalatra" %% "scalatra-specs2" % ScalatraVersion % "test" withJavadoc(),
        "org.scalatra" %% "scalatra-json" % ScalatraVersion withJavadoc(),
        "org.scalatra" %% "scalatra-auth" % ScalatraVersion withJavadoc(),
        "ch.qos.logback" % "logback-classic" % "1.0.6" % "runtime" withJavadoc(),
        "org.eclipse.jetty" % "jetty-webapp" % "8.1.10.v20130312" % "container;compile" withJavadoc(),
        "org.eclipse.jetty.orbit" % "javax.servlet" % "3.0.0.v201112011016" % "container;provided;test" artifacts (Artifact("javax.servlet", "jar", "jar")),
        "c3p0" % "c3p0" % "0.9.1.2" withJavadoc(),
        "org.json4s" %% "json4s-jackson" % "3.2.6" withJavadoc(),
        "com.typesafe.slick" %% "slick" % "2.0.2" withJavadoc(),
        "com.typesafe.slick" %% "slick-extensions" % "2.0.2" withJavadoc(),
        "com.typesafe.akka" %% "akka-actor" % "2.3.4",
        "net.databinder.dispatch" %% "dispatch-core" % "0.11.1",
        "com.restfb" % "restfb" % "1.10.1",
        "org.twitter4j" % "twitter4j-core" % "4.0.2",
        "postgresql" % "postgresql" % "9.1-901.jdbc4",
        "javax.mail" % "mail" % "1.4.1",
        "com.google.api-client" % "google-api-client" % "1.19.1",
        "com.google.code.gson" % "gson" % "2.3.1",
        "com.google.oauth-client" % "google-oauth-client" % "1.19.0",
        "com.google.http-client" % "google-http-client-gson" % "1.19.0",
        "com.google.apis" % "google-api-services-analytics" % "v3-rev111-1.19.1",
        "com.likethecolor" % "alchemy" % "1.2.0"

        //"org.joda" %% "joda-time" % "2.2"  ,
        //"org.joda" %% "joda-convert" % "1.6"
      ),

      unmanagedJars in Compile ++= {
        val base = baseDirectory.value
        val customJars = (base / "custom_lib" ** "*.jar")
        customJars.classpath
      },

      scalacOptions ++= Seq("-deprecation", "-feature"),

      scalateTemplateConfig in Compile <<= (sourceDirectory in Compile) { base =>
        Seq(
          TemplateConfig(
            base / "webapp" / "WEB-INF" / "templates",
            Seq.empty, /* default imports should be added here */
            Seq(
              Binding("context", "_root_.org.scalatra.scalate.ScalatraRenderContext", importMembers = true, isImplicit = true)
            ), /* add extra bindings here */
            Some("templates")
          )
        )
      }
    )
  )
}
