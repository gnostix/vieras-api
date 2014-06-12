package gr.gnostix.api.servlets

import gr.gnostix.api.GnostixAPIStack
import org.scalatra.{CorsSupport, ScalatraServlet}
import org.scalatra.json.JacksonJsonSupport
import gr.gnostix.api.auth.AuthenticationSupport
import org.json4s.{DefaultFormats, Formats}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import gr.gnostix.api.models._
import gr.gnostix.api.models.DataResponse
import gr.gnostix.api.models.AllDataResponse

trait RestDatafindingsRoutes extends ScalatraServlet
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


  get("/profile/:profileId/line/stats/all/:fromDate/:toDate") {
    logger.info(s"---->   /datafindings/profile/:profileId/line/stats/all ${params("fromDate")}  ${params("toDate")}  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt

      val myDataList = List(DtTwitterLineGraphDAO.getLineData(fromDate, toDate, profileId),
        DtFacebookLineGraphDAO.getLineData(fromDate, toDate, profileId),
        DtGoogleplusLineGraphDAO.getLineData(fromDate, toDate, profileId),
        DtYoutubeLineGraphDAO.getLineData(fromDate, toDate, profileId),
        DtWebLineGraphDAO.getLineData(fromDate, toDate, profileId),
        DtFeedLineGraphDAO.getLineData(fromDate, toDate, profileId, FeedDatasources.blogs),
        DtFeedLineGraphDAO.getLineData(fromDate, toDate, profileId, FeedDatasources.news),
        DtFeedLineGraphDAO.getLineData(fromDate, toDate, profileId, FeedDatasources.personal)
      )
      AllDataResponse(200, "Ola Pigan Kala!!!", myDataList)

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }

  get("/profile/:profileId/line/stats/twitter/:fromDate/:toDate") {
    logger.info(s"---->   /datafindings/twitter ${params("fromDate")}  ${params("toDate")}  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val lineData = DtTwitterLineGraphDAO.getLineData(fromDate, toDate, profileId)
      DataResponse(200, "Bravo malaka!!!", lineData)
    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }

  get("/profile/:profileId/line/stats/facebook/:fromDate/:toDate") {
    logger.info(s"---->   /datafindings/facebook ${params("fromDate")}  ${params("toDate")}  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))

      val profileId = params("profileId").toInt

      logger.info(s"---->   parsed date ---> ${toDate}    ")
      val lineData = DtFacebookLineGraphDAO.getLineData(fromDate, toDate, profileId)
      DataResponse(200, "Bravo malaka!!!", lineData)

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }

  get("/profile/:profileId/line/stats/gplus/:fromDate/:toDate") {
    logger.info(s"---->   /datafindings/gplus ${params("fromDate")}  ${params("toDate")}  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      val profileId = params("profileId").toInt

      logger.info(s"---->   parsed date ---> ${toDate}    ")
      val lineData = DtGoogleplusLineGraphDAO.getLineData(fromDate, toDate, profileId)
      DataResponse(200, "Bravo malaka!!!", lineData)

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }

  get("/profile/:profileId/line/stats/web/:fromDate/:toDate") {
    logger.info(s"---->   /datafindings/web ${params("fromDate")}  ${params("toDate")}  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")
      val profileId = params("profileId").toInt

      val lineData = DtWebLineGraphDAO.getLineData(fromDate, toDate, profileId)
      DataResponse(200, "Bravo malaka!!!", lineData)

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }

  get("/profile/:profileId/line/stats/youtube/:fromDate/:toDate") {
    logger.info(s"---->   /datafindings/youtube ${params("fromDate")}  ${params("toDate")}  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")
      val profileId = params("profileId").toInt

      val lineData = DtYoutubeLineGraphDAO.getLineData(fromDate, toDate, profileId)
      DataResponse(200, "Bravo malaka!!!", lineData)
      //user.userId
    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }

  get("/profile/:profileId/line/stats/feed/:type/:fromDate/:toDate") {
    logger.info(s"---->   /datafindings/feed ${params("fromDate")}  ${params("toDate")}  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")
      val profileId = params("profileId").toInt

      if (params("type") == "blogs") {

      } else if (params("type") == "blogs") {
        val lineData = DtFeedLineGraphDAO.getLineData(fromDate, toDate, profileId, FeedDatasources.blogs)
      }
      val lineData: SocialData = params("type") match {
        case "blogs" =>  DtFeedLineGraphDAO.getLineData(fromDate, toDate, profileId, FeedDatasources.blogs)
        case "news" =>  DtFeedLineGraphDAO.getLineData(fromDate, toDate, profileId, FeedDatasources.news)
        case "personal" =>  DtFeedLineGraphDAO.getLineData(fromDate, toDate, profileId, FeedDatasources.personal)
      }
      DataResponse(200, "Bravo malaka!!!", lineData)

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }


}

case class DatafindingsServlet() extends GnostixAPIStack with RestDatafindingsRoutes
