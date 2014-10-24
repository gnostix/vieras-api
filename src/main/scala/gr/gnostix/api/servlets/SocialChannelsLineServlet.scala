package gr.gnostix.api.servlets


import gr.gnostix.api.GnostixAPIStack
import org.scalatra.{FutureSupport, AsyncResult, CorsSupport, ScalatraServlet}
import org.scalatra.json.JacksonJsonSupport
import gr.gnostix.api.auth.AuthenticationSupport
import org.json4s.{DefaultFormats, Formats}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import gr.gnostix.api.models._

import scala.concurrent.{Future, ExecutionContext}

trait RestSocialChannelsLineDataRoutes extends GnostixAPIStack
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
    //requireLogin()
  }

  // mount point /api/user/socialchannels/line/*

  get("/profile/:profileId/facebook/:dataType/:fromDate/:toDate") {

  }

  // get all data for facebook for  one account datatype = (all, post, comment)
  get("/profile/:profileId/facebook/:dataType/:queryId/:fromDate/:toDate") {
    logger.info(s"----> get all data for facebook for  one account datatype = (all, post, comment)" +
      s"  /api/user/socialchannels/line/* ${params("dataType")} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt

      val rawData = MySocialChannelDaoFB.getLineCounts(fromDate, toDate, profileId, params("dataType"))
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

 // get all data for facebook for  all accounts datatype = (all, post, comment)
//  get("/profile/:profileId/facebook/:fromDate/:toDate/all") {
//    logger.info(s"---->   /api/user/socialchannels/line/* ${params("dataType")} ")
//    try {
//      val fromDate: DateTime = DateTime.parse(params("fromDate"),
//        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
//      logger.info(s"---->   parsed date ---> ${fromDate}    ")
//
//      val toDate: DateTime = DateTime.parse(params("toDate"),
//        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
//      logger.info(s"---->   parsed date ---> ${toDate}    ")
//
//      val profileId = params("profileId").toInt
//
//
//      val post = MySocialChannelDaoFB.getLineAllData(executor, fromDate, toDate, profileId, "post")
//      val comment = MySocialChannelDaoFB.getLineAllData(executor, fromDate, toDate, profileId, "comment")
//
//      val rawData =
//        new AsyncResult() {
//          override val is =
//            for {
//              a1 <- post
//              a2 <- comment
//            } yield f1(List(a1.get, a2.get))
//          is match {
//            case Some(is.data) => DataResponseAccounts(200, "Coulio Bro!!!", data.asInstanceOf[List[SocialData]])
//            case None => ErrorDataResponse(404, "Error on data")
//          }
//        }
//    } catch {
//      case e: NumberFormatException => "wrong profile number"
//      case e: Exception => {
//        logger.info(s"-----> ${e.printStackTrace()}")
//        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
//      }
//    }
//  }


  def f1(allSocialData: List[SocialData]) = {
    val mydata = allSocialData.map(_.data).flatten

    val k = (allSocialData.map(_.data).flatten).groupBy(_.asInstanceOf[DataLineGraph].msgDate).map {
      case (key, msgList) => (key, msgList.map(_.asInstanceOf[DataLineGraph].msgNum).sum)
    }.map {
      case (x, y) => new DataLineGraph(y, x)
    }

  }


  // get all data for facebook for  all accounts datatype = (all, post, comment)
  get("/profile/:profileId/facebook/:dataType/:fromDate/:toDate") {
    logger.info(s"---->   /api/user/socialchannels/line/* ${
      params("dataType")
    } ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${
        fromDate
      }    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${
        toDate
      }    ")

      val profileId = params("profileId").toInt

      val rawData: Option[SocialData] = params("dataType") match {
        case "post" => MySocialChannelDaoFB.getLineCounts(fromDate, toDate, profileId, params("dataType"))
        case "comment" => MySocialChannelDaoFB.getLineCounts(fromDate, toDate, profileId, params("dataType"))
      }

      rawData match {
        case Some(data) => DataResponse(200, "Coulio Bro!!!", rawData.get)
        case None => ErrorDataResponse(404, "Error on data")
      }

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${
          e.printStackTrace()
        }")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }


  get("/profile/:profileId/:datasource/:fromDate/:toDate") {
    logger.info(s"---->   sentiment /sentiment/:datasource/:msgId ${
      params("datasource")
    } ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${
        fromDate
      }    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${
        toDate
      }    ")

      val profileId = params("profileId").toInt

      val rawData = DatafindingsSentimentLineDao.getDataDefault(fromDate, toDate, profileId, params("datasource"))
      rawData match {
        case Some(data) => DataResponse(200, "Coulio Bro!!!", rawData.get)
        case None => ErrorDataResponse(404, "Error on data")
      }

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${
          e.printStackTrace()
        }")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }


}

case class SocialChannelsLineServlet(executor: ExecutionContext) extends GnostixAPIStack with RestSocialChannelsLineDataRoutes
