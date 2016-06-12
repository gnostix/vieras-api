package gr.gnostix.api.servlets.publicSearchServlet

import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.models.plainModels.DataResponse
import gr.gnostix.api.models.publicSearch.DatafindingsThirdLevelDataDAO
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json.JacksonJsonSupport
import org.scalatra.{CorsSupport, ScalatraServlet}

trait RestDatafindingsThirdLevelDataRoutes extends ScalatraServlet
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
    requireLogin()
  }

  // mount point "/api/user/datafindings/raw/thirdlevel/*

  get("/profile/:profileId/data/:datasource/:fromDate/:toDate") {
    logger.info(s"---->   Third level /data/:datasource/:msgId ${params("datasource")} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val rawData = DatafindingsThirdLevelDataDAO.getThirdLevelDataDefault(fromDate, toDate, user.userId, profileId, params("datasource"))
      DataResponse(200, "Coulio Bro!!!", rawData)
    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }


  post("/profile/:profileId/data/:datasource/:fromDate/:toDate/:keyortopic") {
    logger.info(s"---->   /datafindings/twitter ${params("fromDate")}  ${params("toDate")}  ")
    val idsList = parsedBody.extract[List[Int]]

    logger.info(s"----> json --> ${idsList} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt

      val lineData = params("keyortopic") match {
        case "keywords" => DatafindingsThirdLevelDataDAO.getThirdLevelDataByKeywords(fromDate, toDate, user.userId, profileId, idsList, params("datasource"))
        case "topics" => DatafindingsThirdLevelDataDAO.getThirdLevelDataByTopics(fromDate, toDate, user.userId, profileId, idsList, params("datasource"))
      }

      DataResponse(200, "Coulio Bro!!!", lineData)
    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }
}

case class DatafindingsThirdLevelDataServlet() extends GnostixAPIStack with RestDatafindingsThirdLevelDataRoutes
