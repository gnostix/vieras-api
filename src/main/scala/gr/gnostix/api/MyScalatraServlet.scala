package gr.gnostix.api

import org.scalatra._
import scalate.ScalateSupport
import gr.gnostix.api.db.plainsql.OraclePlainSQLQueries
import scala.slick.jdbc.JdbcBackend.Database
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json._
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.models.{UserDao, User}


trait RestApiRoutes extends ScalatraServlet
with UserDao
with JacksonJsonSupport
with AuthenticationSupport
with CorsSupport {

  options("/*") {
    response.setHeader("Access-Control-Allow-Headers", request.getHeader("Access-Control-Request-Headers"));
  }

  // Sets up automatic case class to JSON output serialization, required by
  // the JValueResult trait.
  protected implicit val jsonFormats: Formats = DefaultFormats

  before() {
    contentType = formats("json")
  }

  post("/login") {
    scentry.authenticate()
    if (isAuthenticated) {
      // logger.info(" logger -------------> /login: successful Name: " + user.age)
      println("--------------> /login: successful Name: " + user.userId)
    } else {
      logger.info("-----------------------> /login: successful" + user.userId)
    }
  }

  get("/data") {
    logger.info("-------------> after getting the data " + user.userId)

    requireLogin()
    List(1, 2, 3, 4, 5)

  }
  get("/data2") {
    //logger.info("-------------> after getting the data2 " + user.userId)

    requireLogin()
    List(1, 2, 3, 4, 5)

  }

  //
  get("/users") {
    //requireLogin()
    // println("----> " + user.age )
    getUsers

    //logger.info("---->        " + user.userId)
  }

  get("/facebook") {
    val fbStats = Map("likes" -> 987664, "comments" -> 6243)
    //List(fbStats)
    //var usr = Customer(100, "Bil", "Vat", 86535)
    //session.put("my_user", usr: Customer)
    logger.info("----> Customer added to session")
    fbStats
  }

  get("/twitter") {
    val twStats = Map("followers" -> 234664, "tweets" -> 6243)
    //val theUser = session.getAttribute("my_user").asInstanceOf[Customer]
    //logger.info("----> get the user from the session : " + theUser.t_results)
    twStats
  }

}

case class MyScalatraServlet(db: Database) extends GnostixAPIStack with RestApiRoutes

