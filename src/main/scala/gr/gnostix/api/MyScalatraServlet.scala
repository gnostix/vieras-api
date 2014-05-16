package gr.gnostix.api

import org.scalatra._
import scalate.ScalateSupport
import gr.gnostix.api.db.plainsql.OraclePlainSQLQueries
import scala.slick.jdbc.JdbcBackend.Database
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json._
import gr.gnostix.api.auth.AuthenticationSupport


trait RestApiRoutes extends ScalatraServlet
  with OraclePlainSQLQueries
  with JacksonJsonSupport
  with AuthenticationSupport {

  // Sets up automatic case class to JSON output serialization, required by
  // the JValueResult trait.
  protected implicit val jsonFormats: Formats = DefaultFormats

  before() {
    contentType = formats("json")
  }

  post("/login") {
    scentry.authenticate()
    if (isAuthenticated) {
      logger.info(" logger -------------> /login: successful Name: " + user.age)
      println("-------------> /login: successful Name: " + user.name)
    } else {
      logger.info("-------------> /login: successful" + user.name)
    }
  }

  get("/data"){
    logger.info("-------------> after getting the data " + user.age)

    requireLogin()
    List(1,2,3,4,5)

  }


  //
  get("/betausers") {
    requireLogin()
    println("----> " + user.age )
    getBetaUsers

    logger.info("---->        " + user.name)
  }

}

 case class MyScalatraServlet(db: Database) extends GnostixAPIStack with RestApiRoutes

