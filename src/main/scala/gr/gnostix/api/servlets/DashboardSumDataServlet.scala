package gr.gnostix.api.servlets

import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.models.oraDao.{MySocialChannelDaoFB, MySocialChannelDaoTw}
import gr.gnostix.api.models.plainModels.{ApiMessages, DataLineGraph, SocialData}
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


  //mount point /api/user/account/dashboard/services/*

  get("/profile/:profileId/social/messages/:fromDate/:toDate") {
    logger.info(s"---->   sentiment /sentiment/comments/ TOTAL from all sources in two groups (social media and hospitality)  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt

      val mentions = MySocialChannelDaoTw.getLineAllData(executor, fromDate, toDate, profileId, "mention", None)
      val retweets = MySocialChannelDaoTw.getLineAllData(executor, fromDate, toDate, profileId, "retweet", None)
      val posts = MySocialChannelDaoFB.getLineAllData(executor, fromDate, toDate, profileId, "post", None)
      val comments = MySocialChannelDaoFB.getLineAllData(executor, fromDate, toDate, profileId, "comment", None)
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

}


  case class DashboardSumDataServlet(executor: ExecutionContext) extends GnostixAPIStack with DashboardSumDataRoutes
