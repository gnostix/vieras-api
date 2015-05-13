package gr.gnostix.api.servlets

import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.models.pgDao.MySocialChannelDaoTw
import gr.gnostix.api.models.plainModels.{ApiData, ApiMessages, ErrorDataResponse}
import gr.gnostix.api.utilities.HelperFunctions
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json.JacksonJsonSupport
import org.scalatra.{AsyncResult, CorsSupport, FutureSupport}

import scala.concurrent.ExecutionContext


trait TwitterDashboardServletRoutes extends GnostixAPIStack
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

  // mount point /api/user/socialchannels/dashboard/twitter/*

  // get all data for twitter for one profile datatype = (post or comment)
  get("/profile/:profileId/company/:companyId/stats/:fromDate/:toDate") {
    logger.info(s"----> get stats  one account " +
      s"  /api/user/socialchannels/dashboard/twitter/*  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt

      val rawDataStats = MySocialChannelDaoTw.getStats(executor, fromDate, toDate, profileId, companyId, None)
      val totalMentions = MySocialChannelDaoTw.getTotalSumData(executor, fromDate, toDate, profileId, companyId, "totalmention", None)
      val totalRetweets = MySocialChannelDaoTw.getTotalSumData(executor, fromDate, toDate, profileId, companyId, "totalretweet", None)
      new AsyncResult() {
        override val is =
          for {
            a1 <- rawDataStats
            a2 <- totalMentions
            a3 <- totalRetweets
          } yield HelperFunctions.f3(Some(List(a1.get, a2.get, a3.get)))
      }

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }

  // get all data for twitter for one profile datatype = (post or comment)
  get("/profile/:profileId/company/:companyId/:credId/stats/:fromDate/:toDate") {
    logger.info(s"----> get stats  one account " +
      s"  /api/user/socialchannels/dashboard/twitter/*  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt
      val credId = params("credId").toInt

      val rawData = MySocialChannelDaoTw.getStats(executor, fromDate, toDate, profileId, companyId, Some(credId))

      new AsyncResult() {
        override val is =
          for {
            a1 <- rawData
          } yield HelperFunctions.f2(a1)
      }

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }





/*
  def f2(dashboardData: Option[ApiData]) = {
    dashboardData match {
      case Some(dt) => {
        val hasData = dt.dataName match {
          case "nodata" => ApiMessages.generalSuccessNoData
          case _ => ApiMessages.generalSuccessOneParam( Map(dt.dataName -> dt.data) )
        }

        hasData
      }
      case None => ErrorDataResponse(404, "Error on data")
    }

  }

  def f3(dashboardData: Option[List[ApiData]]) = {
    dashboardData match {
      case Some(dt) => {

        val existData = dt.filter(_.dataName != "nodata")

        val koko = existData.map{
          case (x) => (x.dataName -> x.data)
        }.toMap

        logger.info(s"----> existData " + existData)
        val hasData = existData.size match {
          case x if( x > 0 ) => ApiMessages.generalSuccessOneParam(koko)
          case x if( x == 0) => ApiMessages.generalSuccessNoData
        }

        hasData
      }
      case None => ErrorDataResponse(404, "Error on data")
    }

  }
*/



  // -------------------- DATA --------------------------

  // get all data for twitter for one profile datatype = (mention, favorite or retweet)
  get("/profile/:profileId/company/:companyId/message/:dataType/:fromDate/:toDate") {
    logger.info(s"----> get text data for twitter for  one account datatype = (post, comment)" +
      s"  /api/user/socialchannels/dashboard/twitter/* ${params("dataType")} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt
      val dataType = params("dataType").toString

      val data = MySocialChannelDaoTw.getTextData(executor, fromDate, toDate, profileId, companyId, dataType, None)


      new AsyncResult() {
        override val is =
          for {
            a1 <- data
          } yield HelperFunctions.f2(a1)
      }

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }



  // get all data for twitter for one channel account datatype = (mention, favorite or retweet)
  get("/profile/:profileId/company/:companyId/message/:dataType/:engId/:fromDate/:toDate") {
    logger.info(s"----> get all data for twitter for  one account datatype = (post, comment)" +
      s"  /api/user/socialchannels/dashboard/twitter/* ${params("dataType")} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt
      val engId = params("engId").toInt
      val dataType = params("dataType").toString

      val data = MySocialChannelDaoTw.getTextData(executor, fromDate, toDate, profileId, companyId, dataType, Some(engId))


      new AsyncResult() {
        override val is =
          for {
            a1 <- data
          } yield HelperFunctions.f2(a1)
      }

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }



  // get all data for twitter for  all accounts datatype = (mention, favorite or retweet)
  get("/profile/:profileId/company/:companyId/message/:fromDate/:toDate/all") {
    logger.info(s"---->  /api/user/socialchannels/dashboard/twitter/* ${params("profileId")} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt

      val mention = MySocialChannelDaoTw.getTextData(executor, fromDate, toDate, profileId, companyId, "mention", None)
      val favorite = MySocialChannelDaoTw.getTextData(executor, fromDate, toDate, profileId, companyId, "favorite", None)
      val retweet = MySocialChannelDaoTw.getTextData(executor, fromDate, toDate, profileId, companyId, "retweet", None)

      val theData =
        new AsyncResult() {
          override val is =
            for {
              a1 <- mention
              a2 <- favorite
              a3 <- retweet
            } yield HelperFunctions.f3(Some(List(a1.get, a2.get, a3.get)))
        }

      theData
    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }

  // get all data for twitter for  one accounts datatype = (mention, favorite or retweet)
  get("/profile/:profileId/company/:companyId/message/:credId/:fromDate/:toDate/all") {
    logger.info(s"----> " +
      s"/api/user/socialchannels/dashboard/twitter/profile/:profileId/message/:credId/:fromDate/:toDate/all ${params("profileId")} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt
      val engId = params("credId").toInt

      val mention = MySocialChannelDaoTw.getTextData(executor, fromDate, toDate, profileId, companyId, "mention", Some(engId))
      val favorite = MySocialChannelDaoTw.getTextData(executor, fromDate, toDate, profileId, companyId, "favorite", Some(engId))
      val retweet = MySocialChannelDaoTw.getTextData(executor, fromDate, toDate, profileId, companyId, "retweet", Some(engId))

      val theData =
        new AsyncResult() {
          override val is =
            for {
              a1 <- mention
              a2 <- favorite
              a3 <- retweet
            } yield HelperFunctions.f3(Some(List(a1.get, a2.get, a3.get)))
        }

      theData
    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }

  // ----------------------------- PEAK DATA -------------------------------------------------------

  // get all data for twitter for one profile datatype = (mention, favorite or retweet)
  get("/profile/:profileId/company/:companyId/message/peak/:dataType/:fromDate/:toDate/:peakDate") {
    logger.info(s"----> get text data for twitter for  peak" +
      s"  /api/user/socialchannels/dashboard/twitter/* ${params("dataType")} ")
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
      val dataType = params("dataType").toString

      val data = MySocialChannelDaoTw.getPeakTextData(executor, fromDate, toDate, peakDate, profileId, companyId, dataType, None)


      new AsyncResult() {
        override val is =
          for {
            a1 <- data
          } yield HelperFunctions.peakSocialMessages(a1)
      }

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }



  // get all data for twitter for one channel account datatype = (mention, favorite or retweet)
  get("/profile/:profileId/company/:companyId/message/peak/:dataType/:engId/:fromDate/:toDate/:peakDate") {
    logger.info(s"----> get all data for twitter for peak" +
      s"  /api/user/socialchannels/dashboard/twitter/* ${params("dataType")} ")
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
      val engId = params("engId").toInt
      val dataType = params("dataType").toString

      val data = MySocialChannelDaoTw.getPeakTextData(executor, fromDate, toDate, peakDate, profileId, companyId, dataType, Some(engId))


      new AsyncResult() {
        override val is =
          for {
            a1 <- data
          } yield HelperFunctions.peakSocialMessages(a1)
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

case class TwitterDashboardServlet(executor: ExecutionContext) extends GnostixAPIStack with TwitterDashboardServletRoutes
