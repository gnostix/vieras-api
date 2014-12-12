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


trait FacebookDashboardServletRoutes extends GnostixAPIStack
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

  // mount point /api/user/socialchannels/dashboard/facebook/*

  // get all data for facebook for one profile datatype = (post or comment)
  get("/profile/:profileId/stats/:fromDate/:toDate") {
    logger.info(s"----> get stats  one account " +
      s"  /api/user/socialchannels/dashboard/facebook/*  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt

      val rawData = MySocialChannelDaoFB.getStats(executor, fromDate, toDate, profileId, None)

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

  // get all data for facebook for one profile datatype = (post or comment)
  get("/profile/:profileId/:credId/stats/:fromDate/:toDate") {
    logger.info(s"----> get stats  one account " +
      s"  /api/user/socialchannels/dashboard/facebook/*  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val credId = params("credId").toInt

      val rawData = MySocialChannelDaoFB.getStats(executor, fromDate, toDate, profileId, Some(credId))

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





  // get all data for facebook for one profile datatype = (post or comment)
  get("/profile/:profileId/demographics/:fromDate/:toDate") {
    logger.info(s"----> get stats  one account " +
      s"  /api/user/socialchannels/dashboard/facebook/*  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt

      val rawData = MySocialChannelDaoFB.getDemographics(executor, fromDate, toDate, profileId, None)

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

  get("/profile/:profileId/:credId/demographics/:fromDate/:toDate") {
    logger.info(s"----> get stats  one account " +
      s"  /api/user/socialchannels/dashboard/facebook/* ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val credId = params("credId").toInt

      val rawData = MySocialChannelDaoFB.getStats(executor, fromDate, toDate, profileId, Some(credId))

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




 // -------------------- DATA --------------------------

  // get all data for facebook for one profile datatype = (post or comment)
  get("/profile/:profileId/message/:dataType/:fromDate/:toDate") {
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

      val data = params("dataType") match {
        case "comment" => MySocialChannelDaoFB.getComments (executor, fromDate, toDate, profileId,  None)
        case "post" => MySocialChannelDaoFB.getComments(executor, fromDate, toDate, profileId, None)
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



  // get all data for facebook for one channel account datatype = (post or comment)
  get("/profile/:profileId/message/:dataType/:engId/:fromDate/:toDate") {
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
  get("/profile/:profileId/message/:fromDate/:toDate/all") {
    logger.info(s"---->   /api/user/socialchannels/facebook/line/* ${params("profileId")} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt


      val post = MySocialChannelDaoFB.getComments(executor, fromDate, toDate, profileId, None)
      val comment = MySocialChannelDaoFB.getComments(executor, fromDate, toDate, profileId, None)

      val theData =
        new AsyncResult() {
          override val is =
            for {
              a1 <- post
              a2 <- comment
            } yield (List(a1))
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

  // get all data for facebook for  all accounts datatype = (all, post, comment)
  get("/profile/:profileId/message/:credId/:fromDate/:toDate/all") {
    logger.info(s"---->   /api/user/socialchannels/facebook/line/* ${params("profileId")} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val engId = params("engId").toInt

      val post = MySocialChannelDaoFB.getLineAllData(executor, fromDate, toDate, profileId, "post", Some(engId))
      val comment = MySocialChannelDaoFB.getLineAllData(executor, fromDate, toDate, profileId, "comment", Some(engId))

      val theData =
        new AsyncResult() {
          override val is =
            for {
              a1 <- post
              a2 <- comment
            } yield (List(a1.get, a2.get))
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

case class FacebookDashboardServlet(executor: ExecutionContext) extends GnostixAPIStack with FacebookDashboardServletRoutes
