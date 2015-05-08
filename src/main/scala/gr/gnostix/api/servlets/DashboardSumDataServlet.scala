package gr.gnostix.api.servlets

import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.models.pgDao.{AppVersionDao, MySocialChannelHotelDao, MySocialChannelDaoFB, MySocialChannelDaoTw}
import gr.gnostix.api.models.plainModels.{ApiData, ApiMessages, DataLineGraph, SocialData}
import gr.gnostix.api.utilities.HelperFunctions
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s.{DefaultFormats, Formats}
import org.scalatra._
import org.scalatra.json.JacksonJsonSupport

import scala.concurrent.ExecutionContext


trait DashboardSumDataRoutes extends ScalatraServlet
with JacksonJsonSupport
with AuthenticationSupport
with CorsSupport
with MethodOverride
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


  //mount point -*

  get("/profile/:profileId/company/:companyId/social/messages/:fromDate/:toDate") {
    logger.info(s"---->   sentiment /sentiment/comments/ TOTAL from all sources in two groups (social media and hospitality)  ")
    try {
      // update also web app version, in session
      val webVersion = AppVersionDao.getWebAppVersion
      session.setAttribute("webversion", webVersion)

      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt

      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val mentions = MySocialChannelDaoTw.getLineAllData(executor, fromDate, toDate, profileId, companyId, "mention", None)
      val retweets = MySocialChannelDaoTw.getLineAllData(executor, fromDate, toDate, profileId, companyId, "retweet", None)
      val posts = MySocialChannelDaoFB.getLineAllData(executor, fromDate, toDate, profileId, companyId, "post", None)
      val comments = MySocialChannelDaoFB.getLineAllData(executor, fromDate, toDate, profileId, companyId, "comment", None)
      //val reviews = MySocialChannelHotelDao.getDataCountsFuture(executor, fromDate, toDate, profileId, "line", None)


      val theData =
        new AsyncResult() {
          override val is =
            for {
              a1 <- mentions
              a2 <- retweets
              a3 <- posts
              a4 <- comments
              //a5 <- reviews
            } yield f1(List(a1.get, a2.get, a3.get, a4.get))
        }

      // return the data
      theData

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss ")
        logger.info(s"-----> ${e.printStackTrace()}")
        ApiMessages.generalError
      }
    }
  }

  def f1(allSocialData: List[SocialData]) = {
    val mydata = allSocialData.map(_.data).flatten

    val k = (allSocialData.map(_.data).flatten).groupBy(_.asInstanceOf[DataLineGraph].msgDate).map {
      case (key, msgList) => (key, msgList.map(_.asInstanceOf[DataLineGraph].msgNum).sum)
    }.map {
      case (x, y) => new DataLineGraph(y, x)
    }
    logger.info(s"-----> kkkkkkkkk ${k}")

    val theData = k.toList
    if(theData.size > 0)
      ApiMessages.generalSuccess("data", theData.sortBy(_.msgDate.getTime) )
    else
      ApiMessages.generalSuccess("data", theData )

  }


  get("/profile/:profileId/company/:companyId/social/messages/text/:fromDate/:toDate") {
    logger.info(s"---->   sentiment /sentiment/comments/ TOTAL from all sources in two groups (social media and hospitality)  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt


      val mentions = MySocialChannelDaoTw.getTextData(executor, fromDate, toDate, profileId, companyId, "mention", None)
      val retweets = MySocialChannelDaoTw.getTextData(executor, fromDate, toDate, profileId, companyId, "retweet", None)
      val favorites = MySocialChannelDaoTw.getTextData(executor, fromDate, toDate, profileId, companyId, "favorite", None)
      val posts = MySocialChannelDaoFB.getTextData(executor, fromDate, toDate, profileId, companyId, "post", None)
      val comments = MySocialChannelDaoFB.getTextData(executor, fromDate, toDate, profileId, companyId, "comment", None)
      val reviews = MySocialChannelHotelDao.getTextData(executor, fromDate, toDate, profileId, companyId, None)


      val theData =
        new AsyncResult() {
          override val is =
            for {
              a1 <- mentions
              a2 <- retweets
              a3 <- favorites
              a4 <- posts
              a5 <- comments
              a6 <- reviews
            } yield HelperFunctions.f3(Some(List(a1.get, a2.get, a3.get, a4.get, a5.get, a6.get)))
        }

      // return the data
      theData

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss ")
        logger.info(s"-----> ${e.printStackTrace()}")
        ApiMessages.generalError
      }
    }
  }

  get("/profile/:profileId/company/:companyId/social/messages/text/hotel/peak/:fromDate/:toDate/:peakDate") {
    logger.info(s"---->   get text for all sources for a peak in the line graph  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val peakDate: DateTime = DateTime.parse(params("peakDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${peakDate}    ")


      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt

      val reviews = MySocialChannelHotelDao.getPeakTextData(executor, fromDate, toDate, peakDate, profileId, companyId, None)


      val theData =
        new AsyncResult() {
          override val is =
            for {
              a6 <- reviews
            } yield HelperFunctions.f3(Some(List(a6.get)))
        }

      // return the data
      theData

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss ")
        logger.info(s"-----> ${e.printStackTrace()}")
        ApiMessages.generalError
      }
    }
  }


  get("/profile/:profileId/company/:companyId/social/messages/text/social/peak/:fromDate/:toDate/:peakDate") {
    logger.info(s"---->   get text for all sources for a peak in the line graph  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val peakDate: DateTime = DateTime.parse(params("peakDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${peakDate}    ")


      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt

      val comments = MySocialChannelDaoFB.getPeakTextData(executor, fromDate, toDate, peakDate, profileId, companyId, "comment", None)
      val posts = MySocialChannelDaoFB.getPeakTextData(executor, fromDate, toDate, peakDate, profileId, companyId, "post", None)
      val retweets = MySocialChannelDaoTw.getPeakTextData(executor, fromDate, toDate, peakDate, profileId, companyId, "retweet", None)
      val mentions = MySocialChannelDaoTw.getPeakTextData(executor, fromDate, toDate, peakDate, profileId, companyId, "mention", None)
      val favorites = MySocialChannelDaoTw.getPeakTextData(executor, fromDate, toDate, peakDate, profileId, companyId, "favorite", None)
      // here we are going to add also posts/ twitter fav, ment, retweets

      val theData =
        new AsyncResult() {
          override val is =
            for {
              a1 <- posts
              a2 <- comments
              a3 <- retweets
              a4 <- mentions
              a5 <- favorites
            } yield  HelperFunctions.fixSumData(Some(List(a1.get, a2.get, a3.get, a4.get, a5.get)))
        }

      // return the data
      theData

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss ")
        logger.info(s"-----> ${e.printStackTrace()}")
        ApiMessages.generalError
      }
    }
  }



  get("/profile/:profileId/company/:companyId/social/messages/text/peak/service/:service/:fromDate/:toDate/:peakDate") {
    logger.info(s"---->   get text for all sources for a peak in the line graph  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val peakDate: DateTime = DateTime.parse(params("peakDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${peakDate}    ")


      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt
      val service = params("service")

      val reviews = MySocialChannelHotelDao.getServicePeakTextData(executor, fromDate, toDate, peakDate, profileId, companyId, None, service)


      val theData =
        new AsyncResult() {
          override val is =
            for {
              a6 <- reviews
            } yield HelperFunctions.f3(Some(List(a6.get)))
        }

      // return the data
      theData

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss ")
        logger.info(s"-----> ${e.printStackTrace()}")
        ApiMessages.generalError
      }
    }
  }

}


  case class DashboardSumDataServlet(executor: ExecutionContext) extends GnostixAPIStack with DashboardSumDataRoutes
