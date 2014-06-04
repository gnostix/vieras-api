package gr.gnostix.api.servlets

import gr.gnostix.api.auth.AuthenticationSupport
import org.scalatra._
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json._
import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.models.{DtTwitterLineGraphDAO, UserDao}


trait RestApiRoutes extends ScalatraServlet
with JacksonJsonSupport
with AuthenticationSupport
with CorsSupport {

  options("/*") {
    response.setHeader("Access-Control-Allow-Headers", request.getHeader("Access-Control-Request-Headers"))
    response.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, HEAD, OPTIONS")
  }

  // Sets up automatic case class to JSON output serialization, required by
  // the JValueResult trait.
  protected implicit val jsonFormats: Formats = DefaultFormats
  //val db: Database

  before() {
    contentType = formats("json")
  }

  post("/login") {
    scentry.authenticate()
    if (isAuthenticated) {
      // logger.info(" logger -------------> /login: successful Name: " + user.age)
      println("--------------> /login: successful Id: " + user.userId)
    } else {
      logger.info("-----------------------> /login: NOT successful")
    }
  }

  get("/data") {
    requireLogin()
    logger.info("-------------> after getting the data " + user.userId)
    List(1, 2, 3, 4, 5)

  }
  get("/data2") {
    //logger.info("-------------> after getting the data2 " + user.userId)

    requireLogin()
    List(1, 2, 3, 4, 5)

  }

  get("/datafindings/twitter") {
    logger.info(s"---->   /datafindings/twitter ${params("fromDate")}  ${params("toDate")}  ")
    DtTwitterLineGraphDAO.getTWLineDataByDay
    //AlexDAO.getAlexNumb
  }

  get("/api/:profileId/datafindings/:topicID/twitter") {
    println(params("profileId"))
    println(params("topicID"))
    println(params("fromDate"))
    println(params("toDate"))

  }

  //
  get("/users") {
    logger.info("---->   ALEX REQUEST     ")
    UserDao.findByUsername("test")
  }

  get("/users1") {
    logger.info("---->   ALEX REQUEST     ")
    UserDao.getUsers
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

/*  get("/jndi") {
    UserDao.getJndi
  }*/
}

case class MyScalatraServlet() extends GnostixAPIStack with RestApiRoutes

