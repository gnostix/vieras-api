

package gr.gnostix.api.servlets.publicSearchServlet

import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.models.plainModels.{DataResponse, ErrorDataResponse, SentimentLine, SocialData}
import gr.gnostix.api.models.publicSearch.{DatafindingsSentimentLineDao, FutureSentimentDao}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json.JacksonJsonSupport
import org.scalatra.{AsyncResult, CorsSupport, FutureSupport}

import scala.concurrent.ExecutionContext

trait RestDatafindingsSentimentLineDataRoutes extends GnostixAPIStack
with JacksonJsonSupport
with AuthenticationSupport
with CorsSupport
with FutureSupport {

  options("/*") {
    response.setHeader("Access-Control-Allow-Headers", request.getHeader("Access-Control-Request-Headers"))
  }

  // Sets up automatic case class to JSON output serialization, required by
  // the JValueResult trait.
  protected implicit val jsonFormats: Formats = DefaultFormats

  before() {
    contentType = formats("json")
    requireLogin()
  }

  // mount point "/api/user/datafindings/sentiment/*



  get("/profile/:profileId/:datasource/:fromDate/:toDate") {
    logger.info(s"---->   sentiment /sentiment/:datasource/:msgId ${params("datasource")} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val rawData = DatafindingsSentimentLineDao.getDataDefault(fromDate, toDate, user.userId, profileId, params("datasource"))
      rawData match {
        case Some(data) => DataResponse(200, "Coulio Bro!!!", rawData.get)
        case None => ErrorDataResponse(404, "Error on data")
      }

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }


  post("/profile/:profileId/:datasource/:fromDate/:toDate/:keyortopic") {
    logger.info(s"---->   /sentiment/twitter ${params("fromDate")}  ${params("toDate")}  ")
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
        case "keywords" => DatafindingsSentimentLineDao.getDataByKeywords(fromDate, toDate, user.userId, profileId, idsList, params("datasource"))
        case "topics" => DatafindingsSentimentLineDao.getDataByTopics(fromDate, toDate, user.userId, profileId, idsList, params("datasource"))
      }

      rawData match {
        case Some(data) => DataResponse(200, "Coulio Bro!!!", rawData.get)
        case None => ErrorDataResponse(404, "Error on data")
      }
    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }


  get("/profile/:profileId/:fromDate/:toDate/all") {

    val fromDate: DateTime = DateTime.parse(params("fromDate"),
      DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
    logger.info(s"---->   parsed date ---> ${fromDate}    ")

    val toDate: DateTime = DateTime.parse(params("toDate"),
      DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
    logger.info(s"---->   parsed date ---> ${toDate}    ")

    val profileId = params("profileId").toInt

    val dt1 = FutureSentimentDao.getDataDefault(executor, fromDate, toDate, user.userId, profileId, "twitter")
    val dt2 = FutureSentimentDao.getDataDefault(executor, fromDate, toDate, user.userId, profileId, "facebook")
    val dt3 = FutureSentimentDao.getDataDefault(executor, fromDate, toDate, user.userId, profileId, "youtube")
    val dt4 = FutureSentimentDao.getDataDefault(executor, fromDate, toDate, user.userId, profileId, "gplus")
    val dt5 = FutureSentimentDao.getDataDefault(executor, fromDate, toDate, user.userId, profileId, "web")
    val dt6 = FutureSentimentDao.getDataDefault(executor, fromDate, toDate, user.userId, profileId, "linkedin")
    val dt7 = FutureSentimentDao.getDataDefault(executor, fromDate, toDate, user.userId, profileId, "news")
    val dt8 = FutureSentimentDao.getDataDefault(executor, fromDate, toDate, user.userId, profileId, "blog")
    val dt9 = FutureSentimentDao.getDataDefault(executor, fromDate, toDate, user.userId, profileId, "personal")
    new AsyncResult {
      val is =
        for {
          a1 <- dt1
          a2 <- dt2
          a3 <- dt3
          a4 <- dt4
          a5 <- dt5
          a6 <- dt6
          a7 <- dt7
          a8 <- dt8
          a9 <- dt9
        } yield f1(List(a1.get, a2.get, a3.get, a4.get, a5.get, a6.get, a7.get, a8.get, a9.get))
    }
  }

  def f1(allSocialData: List[SocialData]) = {

    val k = (allSocialData.map(_.data).flatten).groupBy(_.asInstanceOf[SentimentLine].sentiment).map {
      case (key, sentimentList) => (key, sentimentList.map(_.asInstanceOf[SentimentLine].msgNum).sum)
    }.map {
      case (x, y) => new SentimentLine(x, y)
    }
    val s = SocialData("all datasources", k.toList)
    DataResponse(200, "Coulio Bro!!!", s)

  }

  post("/profile/:profileId/:fromDate/:toDate/all/:keyortopic") {
    logger.info(s"---->   /sentiment/all ${params("fromDate")}  ${params("toDate")}  ")
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

      params("keyortopic") match {
        case "keywords" => {
          val dt1 = FutureSentimentDao.getDataByKeywords(executor, fromDate, toDate, user.userId, profileId, idsList, "twitter")
          val dt2 = FutureSentimentDao.getDataByKeywords(executor, fromDate, toDate, user.userId, profileId, idsList, "facebook")
          val dt3 = FutureSentimentDao.getDataByKeywords(executor, fromDate, toDate, user.userId, profileId, idsList, "youtube")
          val dt4 = FutureSentimentDao.getDataByKeywords(executor, fromDate, toDate, user.userId, profileId, idsList, "gplus")
          val dt5 = FutureSentimentDao.getDataByKeywords(executor, fromDate, toDate, user.userId, profileId, idsList, "web")
          val dt6 = FutureSentimentDao.getDataByKeywords(executor, fromDate, toDate, user.userId, profileId, idsList, "linkedin")
          val dt7 = FutureSentimentDao.getDataByKeywords(executor, fromDate, toDate, user.userId, profileId, idsList, "news")
          val dt8 = FutureSentimentDao.getDataByKeywords(executor, fromDate, toDate, user.userId, profileId, idsList, "blog")
          val dt9 = FutureSentimentDao.getDataByKeywords(executor, fromDate, toDate, user.userId, profileId, idsList, "personal")
          new AsyncResult {
            val is =
              for {
                a1 <- dt1
                a2 <- dt2
                a3 <- dt3
                a4 <- dt4
                a5 <- dt5
                a6 <- dt6
                a7 <- dt7
                a8 <- dt8
                a9 <- dt9
              } yield f1(List(a1.get, a2.get, a3.get, a4.get, a5.get, a6.get, a7.get, a8.get, a9.get))
          }
        }
        case "topics" => {
          val dt1 = FutureSentimentDao.getDataByTopics(executor, fromDate, toDate, user.userId, profileId, idsList, "twitter")
          val dt2 = FutureSentimentDao.getDataByTopics(executor, fromDate, toDate, user.userId, profileId, idsList, "facebook")
          val dt3 = FutureSentimentDao.getDataByTopics(executor, fromDate, toDate, user.userId, profileId, idsList, "youtube")
          val dt4 = FutureSentimentDao.getDataByTopics(executor, fromDate, toDate, user.userId, profileId, idsList, "gplus")
          val dt5 = FutureSentimentDao.getDataByTopics(executor, fromDate, toDate, user.userId, profileId, idsList, "web")
          val dt6 = FutureSentimentDao.getDataByTopics(executor, fromDate, toDate, user.userId, profileId, idsList, "linkedin")
          val dt7 = FutureSentimentDao.getDataByTopics(executor, fromDate, toDate, user.userId, profileId, idsList, "news")
          val dt8 = FutureSentimentDao.getDataByTopics(executor, fromDate, toDate, user.userId, profileId, idsList, "blog")
          val dt9 = FutureSentimentDao.getDataByTopics(executor, fromDate, toDate, user.userId, profileId, idsList, "personal")
          new AsyncResult {
            val is =
              for {
                a1 <- dt1
                a2 <- dt2
                a3 <- dt3
                a4 <- dt4
                a5 <- dt5
                a6 <- dt6
                a7 <- dt7
                a8 <- dt8
                a9 <- dt9
              } yield f1(List(a1.get, a2.get, a3.get, a4.get, a5.get, a6.get, a7.get, a8.get, a9.get))
          }
        }
        case _ => None
      }

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }




}

case class DatafindingsSentimentLineServlet(executor: ExecutionContext) extends GnostixAPIStack with RestDatafindingsSentimentLineDataRoutes

