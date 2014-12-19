package gr.gnostix.api.servlets

import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.models._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json.JacksonJsonSupport
import org.scalatra.{AsyncResult, CorsSupport, FutureSupport}

import scala.concurrent.ExecutionContext


trait YoutubeDashboardServletRoutes extends GnostixAPIStack
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


  /**
   *  The stats part is done. Now I have to make the calls for the graph and the top video listed
   *
   */


  // mount point /api/user/socialchannels/dashboard/youtube/*

  // get all data for youtube for one profile datatype = (post or comment)
  get("/profile/:profileId/stats/:fromDate/:toDate") {
    logger.info(s"----> get stats  one account " +
      s"  /api/user/socialchannels/dashboard/youtube/*  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt

      val rawDataStats = MySocialChannelDaoYt.getStats(executor, fromDate, toDate, profileId, None)
      val rawDataVideoStats = MySocialChannelDaoYt.getVideoStats(executor, fromDate, toDate, profileId, None)

      new AsyncResult() {
        override val is =
          for {
            a1 <- rawDataStats
            a2 <- rawDataVideoStats

          } yield f3(Some(List(a1.get, a2.get)))
      }

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }

  // get all data for youtube for one profile datatype = (post or comment)
  get("/profile/:profileId/:credId/stats/:fromDate/:toDate") {
    logger.info(s"----> get stats  one account " +
      s"  /api/user/socialchannels/dashboard/youtube/*  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val credId = params("credId").toInt

      val rawData = MySocialChannelDaoYt.getStats(executor, fromDate, toDate, profileId, Some(credId))

      new AsyncResult() {
        override val is =
          for {
            a1 <- rawData
          } yield f2(a1)
      }

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }





  def f2(dashboardData: Option[ApiData]) = {
    dashboardData match {
      case Some(dt) => {

        val hasData = dt.dataName match {
          case "nodata" => ApiMessages.generalSuccessNoData
          case _ => ApiMessages.generalSuccessOneParam(dt)
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



  // -------------------- DATA --------------------------

  // get all data for youtube for one profile datatype = (mention, favorite or retweet)
  get("/profile/:profileId/message/:dataType/:fromDate/:toDate") {
    logger.info(s"----> get text data for youtube for  one account datatype = (post, comment)" +
      s"  /api/user/socialchannels/dashboard/youtube/* ${params("dataType")} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val dataType = params("dataType").toString

      val data = MySocialChannelDaoYt.getTextData(executor, fromDate, toDate, profileId, dataType, None)


      new AsyncResult() {
        override val is =
          for {
            a1 <- data
          } yield f2(a1)
      }

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }



  // get all data for youtube for one channel account datatype = (mention, favorite or retweet)
  get("/profile/:profileId/message/:dataType/:engId/:fromDate/:toDate") {
    logger.info(s"----> get all data for youtube for  one account datatype = (post, comment)" +
      s"  /api/user/socialchannels/dashboard/youtube/* ${params("dataType")} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val engId = params("engId").toInt
      val dataType = params("dataType").toString

      val data = MySocialChannelDaoYt.getTextData(executor, fromDate, toDate, profileId, dataType, Some(engId))


      new AsyncResult() {
        override val is =
          for {
            a1 <- data
          } yield f2(a1)
      }

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }



  // get all data for youtube for  all accounts datatype = (mention, favorite or retweet)
  get("/profile/:profileId/message/:fromDate/:toDate/all") {
    logger.info(s"---->  /api/user/socialchannels/dashboard/youtube/* ${params("profileId")} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt

      val mention = MySocialChannelDaoYt.getTextData(executor, fromDate, toDate, profileId, "mention", None)
      val favorite = MySocialChannelDaoYt.getTextData(executor, fromDate, toDate, profileId, "favorite", None)
      val retweet = MySocialChannelDaoYt.getTextData(executor, fromDate, toDate, profileId, "retweet", None)

      val theData =
        new AsyncResult() {
          override val is =
            for {
              a1 <- mention
              a2 <- favorite
              a3 <- retweet
            } yield f3(Some(List(a1.get, a2.get, a3.get)))
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

  // get all data for youtube for  one accounts datatype = (mention, favorite or retweet)
  get("/profile/:profileId/message/:credId/:fromDate/:toDate/all") {
    logger.info(s"----> " +
      s"/api/user/socialchannels/dashboard/youtube/profile/:profileId/message/:credId/:fromDate/:toDate/all ${params("profileId")} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val engId = params("credId").toInt

      val mention = MySocialChannelDaoYt.getTextData(executor, fromDate, toDate, profileId, "mention", Some(engId))
      val favorite = MySocialChannelDaoYt.getTextData(executor, fromDate, toDate, profileId, "favorite", Some(engId))
      val retweet = MySocialChannelDaoYt.getTextData(executor, fromDate, toDate, profileId, "retweet", Some(engId))

      val theData =
        new AsyncResult() {
          override val is =
            for {
              a1 <- mention
              a2 <- favorite
              a3 <- retweet
            } yield f3(Some(List(a1.get, a2.get, a3.get)))
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




}

case class YoutubeDashboardServlet(executor: ExecutionContext) extends GnostixAPIStack with YoutubeDashboardServletRoutes
