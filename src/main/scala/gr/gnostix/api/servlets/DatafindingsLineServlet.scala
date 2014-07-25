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

trait RestDatafindingsLineRoutes extends ScalatraServlet
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

  // mount point /api/user/datafindings/line/*


  // this has to be ASYNC !!

/*  get("/profile/:profileId/line/stats/all/:fromDate/:toDate") {
    logger.info(s"---->   /datafindings/profile/:profileId/line/stats/all ${params("fromDate")}  ${params("toDate")}  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt

      val myDataList = List(
        DtTwitterLineGraphDAO.getLineDataDefault(fromDate, toDate, profileId),
        DtFacebookLineGraphDAO.getLineDataDefault(fromDate, toDate, profileId),
        DtGoogleplusLineGraphDAO.getLineDataDefault(fromDate, toDate, profileId),
        DtYoutubeLineGraphDAO.getLineDataDefault(fromDate, toDate, profileId),
        DtWebLineGraphDAO.getLineDataDefault(fromDate, toDate, profileId, WebDatasources.web),
        DtWebLineGraphDAO.getLineDataDefault(fromDate, toDate, profileId, WebDatasources.linkedin),
        DtFeedLineGraphDAO.getLineDataDefault(fromDate, toDate, profileId, FeedDatasources.blogs),
        DtFeedLineGraphDAO.getLineDataDefault(fromDate, toDate, profileId, FeedDatasources.news),
        DtFeedLineGraphDAO.getLineDataDefault(fromDate, toDate, profileId, FeedDatasources.personal)
      )
      AllDataResponse(200, "Ola Pigan Kala!!!", myDataList)

    } catch {
      case e: NumberFormatException =>
        AllDataResponse(444, "problem!!!", List())
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        AllDataResponse(444, "problem!!!", List())
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }*/

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
      val lineData = DtTwitterLineGraphDAO.getLineDataDefault(fromDate, toDate, profileId)
      DataResponse(200, "Coulio Bro!!!", lineData)
    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }


  post("/profile/:profileId/line/stats/twitter/:fromDate/:toDate/:keyortopic") {
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
        case "keywords" => DtTwitterLineGraphDAO.getLineDataByKeywords(fromDate, toDate, profileId, idsList)
        case "topics" => DtTwitterLineGraphDAO.getLineDataByTopics(fromDate, toDate, profileId, idsList)
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
      val lineData = DtFacebookLineGraphDAO.getLineDataDefault(fromDate, toDate, profileId)
      DataResponse(200, "Coulio Bro!!!", lineData)

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }

  post("/profile/:profileId/line/stats/facebook/:fromDate/:toDate/:keyortopic") {
    logger.info(s"---->   /datafindings/facebook ${params("fromDate")}  ${params("toDate")}  ")
    val idsList = parsedBody.extract[List[Int]]

    logger.info(s"----> json --> ${idsList} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))

      val profileId = params("profileId").toInt

      logger.info(s"---->   parsed date ---> ${toDate}    ")
      val lineData = params("keyortopic") match {
        case "keywords" => DtFacebookLineGraphDAO.getLineDataByKeywords(fromDate, toDate, profileId, idsList)
        case "topics" => DtFacebookLineGraphDAO.getLineDataByTopics(fromDate, toDate, profileId, idsList)
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
      val lineData = DtGoogleplusLineGraphDAO.getLineDataDefault(fromDate, toDate, profileId)
      DataResponse(200, "Coulio Bro!!!", lineData)

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }

  post("/profile/:profileId/line/stats/gplus/:fromDate/:toDate/:keyortopic") {
    logger.info(s"---->   /datafindings/gplus ${params("fromDate")}  ${params("toDate")}  ")
    val idsList = parsedBody.extract[List[Int]]
    logger.info(s"----> json --> ${idsList} ")

    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      val profileId = params("profileId").toInt

      logger.info(s"---->   parsed date ---> ${toDate}    ")
      val lineData = params("keyortopic") match {
        case "keywords" => DtGoogleplusLineGraphDAO.getLineDataByKeywords(fromDate, toDate, profileId, idsList)
        case "topics" => DtGoogleplusLineGraphDAO.getLineDataByTopics(fromDate, toDate, profileId, idsList)
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

      val lineData = DtYoutubeLineGraphDAO.getLineDataDefault(fromDate, toDate, profileId)
      DataResponse(200, "Coulio Bro!!!", lineData)
      //user.userId
    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }


  post("/profile/:profileId/line/stats/youtube/:fromDate/:toDate/:keyortopic") {
    logger.info(s"---->   /datafindings/youtube ${params("fromDate")}  ${params("toDate")}  ")
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
        case "keywords" => DtYoutubeLineGraphDAO.getLineDataByKeywords(fromDate, toDate, profileId, idsList)
        case "topics" => DtYoutubeLineGraphDAO.getLineDataByTopics(fromDate, toDate, profileId, idsList)
      }
      DataResponse(200, "Coulio Bro!!!", lineData)
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

      val lineData: SocialData = params("type") match {
        case "blogs" => DtFeedLineGraphDAO.getLineDataDefault(fromDate, toDate, profileId, FeedDatasources.blogs)
        case "news" => DtFeedLineGraphDAO.getLineDataDefault(fromDate, toDate, profileId, FeedDatasources.news)
        case "personal" => DtFeedLineGraphDAO.getLineDataDefault(fromDate, toDate, profileId, FeedDatasources.personal)
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

  post("/profile/:profileId/line/stats/feed/:type/:fromDate/:toDate/:keyortopic") {
    logger.info(s"---->   /datafindings/feed ${params("fromDate")}  ${params("toDate")}  ")
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

      val lineData: SocialData = params("type") match {
        case "blogs" => params("keyortopic") match {
          case "keywords" => DtFeedLineGraphDAO.getLineDataByKeywords(fromDate, toDate, profileId, idsList, FeedDatasources.blogs)
          case "topics" => DtFeedLineGraphDAO.getLineDataByTopics(fromDate, toDate, profileId, idsList, FeedDatasources.blogs)
        }
        case "news" => params("keyortopic") match {
          case "keywords" => DtFeedLineGraphDAO.getLineDataByKeywords(fromDate, toDate, profileId, idsList, FeedDatasources.news)
          case "topics" => DtFeedLineGraphDAO.getLineDataByTopics(fromDate, toDate, profileId, idsList, FeedDatasources.news)
        }
        case "personal" => params("keyortopic") match {
          case "keywords" => DtFeedLineGraphDAO.getLineDataByKeywords(fromDate, toDate, profileId, idsList, FeedDatasources.personal)
          case "topics" => DtFeedLineGraphDAO.getLineDataByTopics(fromDate, toDate, profileId, idsList, FeedDatasources.personal)
        }
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

  get("/profile/:profileId/line/stats/web/:websource/:fromDate/:toDate") {
    logger.info(s"---->   /datafindings/web ${params("fromDate")}  ${params("toDate")}  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")
      val profileId = params("profileId").toInt

      val lineData: SocialData = params("websource") match {
        case "web" => DtWebLineGraphDAO.getLineDataDefault(fromDate, toDate, profileId, WebDatasources.web)
        case "linkedin" => DtWebLineGraphDAO.getLineDataDefault(fromDate, toDate, profileId, WebDatasources.linkedin)
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

  post("/profile/:profileId/line/stats/web/:websource/:fromDate/:toDate/:keyortopic") {
    logger.info(s"---->   /datafindings/web ${params("fromDate")}  ${params("toDate")}  ")
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

      val lineData: SocialData = params("websource") match {
        case "web" => params("keyortopic") match {
          case "keywords" => DtWebLineGraphDAO.getLineDataByKeywords(fromDate, toDate, profileId, idsList, WebDatasources.web)
          case "topics" => DtWebLineGraphDAO.getLineDataByTopics(fromDate, toDate, profileId, idsList, WebDatasources.linkedin)
        }
        case "linkedin" => params("keyortopic") match {
          case "keywords" => DtWebLineGraphDAO.getLineDataByKeywords(fromDate, toDate, profileId, idsList, WebDatasources.web)
          case "topics" => DtWebLineGraphDAO.getLineDataByTopics(fromDate, toDate, profileId, idsList, WebDatasources.linkedin)
        }
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

case class DatafindingsLineServlet() extends GnostixAPIStack with RestDatafindingsLineRoutes