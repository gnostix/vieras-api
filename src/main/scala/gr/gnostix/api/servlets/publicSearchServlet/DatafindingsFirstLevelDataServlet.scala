
package gr.gnostix.api.servlets.publicSearchServlet

import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.models.plainModels.{DataResponse, SocialData}
import gr.gnostix.api.models.publicSearch.{DatafindingsFirstLevelDataDAO, FeedDatasources, WebDatasources}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json.JacksonJsonSupport
import org.scalatra.{CorsSupport, ScalatraServlet}

trait RestDatafindingsFirstLevelDataRoutes extends ScalatraServlet
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

  //mount point /api/user/datafindings/raw/firstlevel/*

  get("/profile/:profileId/data/stats/all/:fromDate/:toDate") {
    /*    logger.info(s"---->   /datafindings/profile/:profileId/data/stats/all ${params("fromDate")}  ${params("toDate")}  ")
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
        }*/
  }


// we consider "social" the following datasources {twitter, facebook, gplus, youtube}
  get("/profile/:profileId/data/stats/social/:datasource/:fromDate/:toDate") {
    logger.info(s"---->   first level /data/stats/twitter ${params("fromDate")}  ${params("toDate")}  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val rawData = DatafindingsFirstLevelDataDAO.getFirstLevelDataDefault(fromDate, toDate, profileId, params("datasource"))
      DataResponse(200, "Coulio Bro!!!", rawData)
    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }


  // we consider "social" the following datasources {twitter, facebook, gplus, youtube}
  post("/profile/:profileId/data/stats/social/:datasource/:fromDate/:toDate/:keyortopic") {
    logger.info(s"----> first level  /data/stats/twitter ${params("fromDate")}  ${params("toDate")}  ")
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

      val rawData = params("keyortopic") match {
        case "keywords" => DatafindingsFirstLevelDataDAO.getFirstLevelDataByKeywords(fromDate, toDate, profileId, idsList, params("datasource"))
        case "topics" => DatafindingsFirstLevelDataDAO.getFirstLevelDataByTopics(fromDate, toDate, profileId, idsList, params("datasource"))
      }

      DataResponse(200, "Coulio Bro!!!", rawData)
    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }




  get("/profile/:profileId/data/stats/feed/:type/:fromDate/:toDate") {
    logger.info(s"---->  first level  /data/stats/feed ${params("fromDate")}  ${params("toDate")}  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")
      val profileId = params("profileId").toInt

      val lineData: SocialData = params("type") match {
        case "blogs" => DatafindingsFirstLevelDataDAO.getFirstLevelDataDefault(fromDate, toDate, profileId, FeedDatasources.blogs.head._2)
        case "news" => DatafindingsFirstLevelDataDAO.getFirstLevelDataDefault(fromDate, toDate, profileId, FeedDatasources.news.head._2)
        case "personal" => DatafindingsFirstLevelDataDAO.getFirstLevelDataDefault(fromDate, toDate, profileId, FeedDatasources.personal.head._2)
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

  post("/profile/:profileId/data/stats/feed/:type/:fromDate/:toDate/:keyortopic") {
    logger.info(s"---->  first level  /data/stats/feed ${params("fromDate")}  ${params("toDate")}  ")
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
          case "keywords" => DatafindingsFirstLevelDataDAO.getFirstLevelDataByKeywords(fromDate, toDate, profileId, idsList, FeedDatasources.blogs.head._2)
          case "topics" => DatafindingsFirstLevelDataDAO.getFirstLevelDataByTopics(fromDate, toDate, profileId, idsList, FeedDatasources.blogs.head._2)
        }
        case "news" => params("keyortopic") match {
          case "keywords" => DatafindingsFirstLevelDataDAO.getFirstLevelDataByKeywords(fromDate, toDate, profileId, idsList, FeedDatasources.news.head._2)
          case "topics" => DatafindingsFirstLevelDataDAO.getFirstLevelDataByTopics(fromDate, toDate, profileId, idsList, FeedDatasources.news.head._2)
        }
        case "personal" => params("keyortopic") match {
          case "keywords" => DatafindingsFirstLevelDataDAO.getFirstLevelDataByKeywords(fromDate, toDate, profileId, idsList, FeedDatasources.personal.head._2)
          case "topics" => DatafindingsFirstLevelDataDAO.getFirstLevelDataByTopics(fromDate, toDate, profileId, idsList, FeedDatasources.personal.head._2)
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

  post("/profile/:profileId/data/stats/web/:websource/:fromDate/:toDate") {
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
        case "web" => DatafindingsFirstLevelDataDAO.getFirstLevelDataDefault(fromDate, toDate, profileId, WebDatasources.web.head._2)
        case "linkedin" => DatafindingsFirstLevelDataDAO.getFirstLevelDataDefault(fromDate, toDate, profileId, WebDatasources.linkedin.head._2)
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

  post("/profile/:profileId/data/stats/web/:websource/:fromDate/:toDate/:keyortopic") {
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
          case "keywords" => DatafindingsFirstLevelDataDAO.getFirstLevelDataByKeywords(fromDate, toDate, profileId, idsList, WebDatasources.web.head._2)
          case "topics" => DatafindingsFirstLevelDataDAO.getFirstLevelDataByTopics(fromDate, toDate, profileId, idsList, WebDatasources.linkedin.head._2)
        }
        case "linkedin" => params("keyortopic") match {
          case "keywords" => DatafindingsFirstLevelDataDAO.getFirstLevelDataByKeywords(fromDate, toDate, profileId, idsList, WebDatasources.web.head._2)
          case "topics" => DatafindingsFirstLevelDataDAO.getFirstLevelDataByTopics(fromDate, toDate, profileId, idsList, WebDatasources.linkedin.head._2)
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

case class DatafindingsFirstLevelDataServlet() extends GnostixAPIStack with RestDatafindingsFirstLevelDataRoutes


