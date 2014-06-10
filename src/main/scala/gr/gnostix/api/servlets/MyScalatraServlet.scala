package gr.gnostix.api.servlets

import gr.gnostix.api.auth.AuthenticationSupport
import org.scalatra._
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json._
import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.models._
import java.util.Date
import java.text.SimpleDateFormat
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import gr.gnostix.api.models.DataResponse


trait RestApiRoutes extends ScalatraServlet
with JacksonJsonSupport
with AuthenticationSupport
with CorsSupport {

  options("/*") {
    response.setHeader("Access-Control-Allow-Headers", request.getHeader("Access-Control-Request-Headers"))
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

  get("/datafindings/line/stats/all/:fromDate/:toDate") {
    logger.info(s"---->   /datafindings/line/stats/all ${params("fromDate")}  ${params("toDate")}  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val myDataList = List(DtTwitterLineGraphDAO.getLineData(fromDate, toDate), DtFacebookLineGraphDAO.getLineData(fromDate, toDate))
      AllDataResponse("200", "Bravo malaka!!!",myDataList)

    } catch {
      case e: Exception => "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
    }
  }

  get("/datafindings/line/stats/twitter/:fromDate/:toDate") {
    logger.info(s"---->   /datafindings/twitter ${params("fromDate")}  ${params("toDate")}  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")
      val lineData = DtTwitterLineGraphDAO.getLineData(fromDate, toDate)
      DataResponse("200", "Bravo malaka!!!",lineData)
    } catch {
      case e: Exception => "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
    }
  }

  get("/datafindings/line/stats/facebook/:fromDate/:toDate") {
    logger.info(s"---->   /datafindings/facebook ${params("fromDate")}  ${params("toDate")}  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")
      val lineData = DtFacebookLineGraphDAO.getLineData(fromDate, toDate)
      DataResponse("200", "Bravo malaka!!!",lineData)

    } catch {
      case e: Exception => "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
    }
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

