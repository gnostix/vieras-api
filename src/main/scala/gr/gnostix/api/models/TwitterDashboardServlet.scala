package gr.gnostix.api.servlets

import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.models._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.{AsyncResult, FutureSupport, CorsSupport}
import org.scalatra.json.JacksonJsonSupport

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
    //requireLogin()
  }

  // mount point /api/user/socialchannels/dashboard/twitter/*

  // get all data for twitter for one profile datatype = (post or comment)
  get("/profile/:profileId/stats/:fromDate/:toDate") {
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

      val rawData = MySocialChannelDaoTw.getStats(executor, fromDate, toDate, profileId, None)

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

  // get all data for twitter for one profile datatype = (post or comment)
  get("/profile/:profileId/:credId/stats/:fromDate/:toDate") {
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
      val credId = params("credId").toInt

      val rawData = MySocialChannelDaoTw.getStats(executor, fromDate, toDate, profileId, Some(credId))

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

        val existData = dt.map(_.dataName).filterNot(m => m != "nodata")

        val hasData = existData.length match {
          case x if (x > 0) => ApiMessages.generalSuccessNoData
          case x if(x == 0) => ApiMessages.generalSuccessOneParam(dt)
        }

        hasData
      }
      case None => ErrorDataResponse(404, "Error on data")
    }

  }



  // -------------------- DATA --------------------------

  // get all data for twitter for one profile datatype = (post or comment)
  get("/profile/:profileId/message/:dataType/:fromDate/:toDate") {
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
      val dataType = params("dataType").toString

      val data = params("dataType") match {
        case "comment" => MySocialChannelDaoTw.getTextData(executor, fromDate, toDate, profileId, dataType, None)
        case "post" => MySocialChannelDaoTw.getTextData(executor, fromDate, toDate, profileId, dataType, None)
      }

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



  // get all data for twitter for one channel account datatype = (post or comment)
  get("/profile/:profileId/message/:dataType/:engId/:fromDate/:toDate") {
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
      val engId = params("engId").toInt
      val dataType = params("dataType").toString

      val data = params("dataType") match {
        case "comment" => MySocialChannelDaoTw.getTextData(executor, fromDate, toDate, profileId, dataType, Some(engId))
        case "post" => MySocialChannelDaoTw.getTextData(executor, fromDate, toDate, profileId, dataType, Some(engId))
      }

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



  // get all data for twitter for  all accounts datatype = (all, post, comment)
  get("/profile/:profileId/message/:fromDate/:toDate/all") {
    logger.info(s"---->  /api/user/socialchannels/dashboard/twitter/* ${params("profileId")} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt

      val post = MySocialChannelDaoTw.getTextData(executor, fromDate, toDate, profileId, "post", None)
      val comment = MySocialChannelDaoTw.getTextData(executor, fromDate, toDate, profileId, "comment", None)

      val theData =
        new AsyncResult() {
          override val is =
            for {
              a1 <- post
              a2 <- comment
            } yield f3(Some(List(a1.get, a2.get)))
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

  // get all data for twitter for  one accounts datatype = (all, post, comment)
  get("/profile/:profileId/message/:credId/:fromDate/:toDate/all") {
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
      val engId = params("credId").toInt

      val post = MySocialChannelDaoTw.getTextData(executor, fromDate, toDate, profileId, "post", Some(engId))
      val comment = MySocialChannelDaoTw.getTextData(executor, fromDate, toDate, profileId, "comment", Some(engId))

      val theData =
        new AsyncResult() {
          override val is =
            for {
              a1 <- post
              a2 <- comment
            } yield f3(Some(List(a1.get, a2.get)))
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

case class TwitterDashboardServlet(executor: ExecutionContext) extends GnostixAPIStack with TwitterDashboardServletRoutes
