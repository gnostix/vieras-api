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
import scala.util.{Failure, Success}

trait RestSocialChannelsFbLineDataRoutes extends GnostixAPIStack
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

  // mount point /api/user/socialchannels/facebook/line/*

  // get all data for facebook for one profile datatype = (post or comment)
  get("/profile/:profileId/:dataType/:fromDate/:toDate") {
    logger.info(s"----> get all data for facebook for  one account datatype = (post, comment)" +
      s"  /api/user/socialchannels/facebook/line/* ${params("dataType")} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt

      val rawData = MySocialChannelDaoFB.getLineCounts(fromDate, toDate, profileId, params("dataType"), None)
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

  // get all data for facebook for one channel account datatype = (post or comment)
  get("/profile/:profileId/:dataType/:engId/:fromDate/:toDate") {
    logger.info(s"----> get all data for facebook for  one account datatype = (post, comment)" +
      s"  /api/user/socialchannels/facebook/line/* ${params("dataType")} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val engId = params("engId").toInt

      val rawData = MySocialChannelDaoFB.getLineCounts(fromDate, toDate, profileId, params("dataType"), Some(engId))
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
  get("/profile/:profileId/:fromDate/:toDate/all") {
    logger.info(s"---->   /api/user/socialchannels/facebook/line/* ${params("profileId")} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt


      val post = MySocialChannelDaoFB.getLineAllData(executor, fromDate, toDate, profileId, "post", None)
      val comment = MySocialChannelDaoFB.getLineAllData(executor, fromDate, toDate, profileId, "comment", None)

      val theData =
        new AsyncResult() {
          override val is =
            for {
              a1 <- post
              a2 <- comment
            } yield f1(List(a1.get, a2.get))
        }
      //return the data

      theData
    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
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

  // get SUM data for facebook for  all accounts datatype = (all, post, comment)
  get("/profile/:profileId/facebook/:fromDate/:toDate/total/all") {
    logger.info(s"---->   /api/user/socialchannels/facebook/line/* ${params("profileId")} ")

    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt

      val post = MySocialChannelDaoFB.getTotalSumData(executor, fromDate, toDate, profileId, "totalpost", None)
      val comment = MySocialChannelDaoFB.getTotalSumData(executor, fromDate, toDate, profileId, "totalcomment", None)

      val theData =
        new AsyncResult() {
          override val is =
            for {
              a1 <- post
              a2 <- comment
            //  } yield (a1.get, a2.get)
            } yield f2(List(a1.get, a2.get))
        }

      //return the data
      theData
    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }

  def f2(a: List[SocialDataSum]) = {
    val theSum = a.groupBy(_.datasource).map {
      case (x, y) => (x, y.map(x => x.data).sum)
    }

    Map("status" -> 200, "message" -> "Coulio Bro!!!", "payload" -> theSum)
  }

  // get all data for facebook for  one account datatype = (all, post, comment)
  get("/profile/:profileId/facebook/:engId/:fromDate/:toDate/total/all") {
    logger.info(s"---->   /api/user/socialchannels/facebook/line/* ${params("engId")} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val engId = params("engId").toInt

      val post = MySocialChannelDaoFB.getTotalSumData(executor, fromDate, toDate, profileId, "totalpost", Some(engId))
      val comment = MySocialChannelDaoFB.getTotalSumData(executor, fromDate, toDate, profileId, "totalcomment", Some(engId))

      val theData =
        new AsyncResult() {
          override val is =
            for {
              a1 <- post
              a2 <- comment
            } yield f2(List(a1.get, a2.get))
        }

      //return the data
      theData
    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }


}

case class SocialChannelsFacebookLineServlet(executor: ExecutionContext) extends GnostixAPIStack with RestSocialChannelsFbLineDataRoutes
