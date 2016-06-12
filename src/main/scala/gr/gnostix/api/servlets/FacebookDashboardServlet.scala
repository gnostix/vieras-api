package gr.gnostix.api.servlets

import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.models.pgDao.MySocialChannelDaoFB
import gr.gnostix.api.models.plainModels.{ApiData, ApiMessages, ErrorDataResponse}
import gr.gnostix.api.utilities.HelperFunctions
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json.JacksonJsonSupport
import org.scalatra.{AsyncResult, CorsSupport, FutureSupport}

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
    requireLogin()
  }

  // mount point /api/user/socialchannels/dashboard/facebook/*

  // get all data for facebook for one profile datatype = (post or comment)
  get("/profile/:profileId/company/:companyId/stats/:fromDate/:toDate") {
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
      val companyId = params("companyId").toInt

      val rawData = MySocialChannelDaoFB.getStats(executor, fromDate, toDate, user.userId, profileId, companyId, None)

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
  get("/profile/:profileId/company/:companyId/:credId/stats/:fromDate/:toDate") {
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
      val companyId = params("companyId").toInt
      val credId = params("credId").toInt

      val rawData = MySocialChannelDaoFB.getStats(executor, fromDate, toDate, user.userId, profileId, companyId, Some(credId))

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
  get("/profile/:profileId/company/:companyId/demographics/:fromDate/:toDate") {
    logger.info(s"----> get demographics  one account " +
      s"  /api/user/socialchannels/dashboard/facebook/*  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt

      val rawData = MySocialChannelDaoFB.getDemographics(executor, fromDate, toDate, user.userId, profileId, companyId, None)

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

  get("/profile/:profileId/company/:companyId/:credId/demographics/:fromDate/:toDate") {
    logger.info(s"----> get demographics  one account " +
      s"  /api/user/socialchannels/dashboard/facebook/* ")
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

      val rawData = MySocialChannelDaoFB.getStats(executor, fromDate, toDate, user.userId, profileId, companyId, Some(credId))

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
          case _ => ApiMessages.generalSuccessOneParam( Map(dt.dataName -> dt.data))
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

  // get all data for facebook for one profile datatype = (post or comment)
  get("/profile/:profileId/company/:companyId/message/:dataType/:fromDate/:toDate") {
    logger.info(s"----> get text data for facebook for  one account datatype = (post, comment)" +
      s"  /api/user/socialchannels/dashboard/facebook/ ${params("dataType")} ")
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

      val data = params("dataType") match {
        case "comment" => MySocialChannelDaoFB.getTextData(executor, fromDate, toDate, user.userId, profileId, companyId, dataType, None)
        case "post" => MySocialChannelDaoFB.getTextData(executor, fromDate, toDate, user.userId, profileId, companyId, dataType, None)
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
  get("/profile/:profileId/company/:companyId/message/:dataType/:engId/:fromDate/:toDate") {
    logger.info(s"----> get all data for facebook for  one account datatype = (post, comment)" +
      s"  /api/user/socialchannels/dashboard/facebook/* ${params("dataType")} ")
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

      val data = params("dataType") match {
        case "comment" => MySocialChannelDaoFB.getTextData(executor, fromDate, toDate, user.userId, profileId, companyId, dataType, Some(engId))
        case "post" => MySocialChannelDaoFB.getTextData(executor, fromDate, toDate, user.userId, profileId, companyId, dataType, Some(engId))
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



  // get all data for facebook for  all accounts datatype = (all, post, comment)
  get("/profile/:profileId/company/:companyId/message/:fromDate/:toDate/all") {
    logger.info(s"---->  /api/user/socialchannels/dashboard/facebook/* ${params("profileId")} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt

      val post = MySocialChannelDaoFB.getTextData(executor, fromDate, toDate, user.userId, profileId, companyId, "post", None)
      val comment = MySocialChannelDaoFB.getTextData(executor, fromDate, toDate, user.userId, profileId, companyId, "comment", None)

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

  // get all data for facebook for  one accounts datatype = (all, post, comment)
  get("/profile/:profileId/company/:companyId/message/:credId/:fromDate/:toDate/all") {
    logger.info(s"----> " +
      s"/api/user/socialchannels/dashboard/facebook/profile/:profileId/message/:credId/:fromDate/:toDate/all ${params("profileId")} ")
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

      val post = MySocialChannelDaoFB.getTextData(executor, fromDate, toDate, user.userId, profileId, companyId, "post", Some(engId))
      val comment = MySocialChannelDaoFB.getTextData(executor, fromDate, toDate, user.userId, profileId, companyId, "comment", Some(engId))

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


  // ----------------------------- PEAK DATA -------------------------------------------------------

  // get all data for facebook for one profile datatype = (post or comment)
  get("/profile/:profileId/company/:companyId/message/peak/:dataType/:fromDate/:toDate/:peakDate") {
    logger.info(s"----> get text data for facebook for  one account datatype = (post, comment)" +
      s"  /api/user/socialchannels/dashboard/facebook/ --peak data  * ${params("dataType")} ")
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

      val data = MySocialChannelDaoFB.getPeakTextData(executor, fromDate, toDate, peakDate, user.userId, profileId, companyId, dataType, None)

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



  // get all data for facebook for one channel account datatype = (post or comment)
  get("/profile/:profileId/company/:companyId/message/peak/:dataType/:engId/:fromDate/:toDate/:peakDate") {
    logger.info(s"----> get all data for facebook for  one account datatype = (post, comment)" +
      s"  /api/user/socialchannels/dashboard/facebook/--peak data * ${params("dataType")} ")
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

      val data = MySocialChannelDaoFB.getPeakTextData(executor, fromDate, toDate, peakDate, user.userId, profileId, companyId, dataType, Some(engId))

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

case class FacebookDashboardServlet(executor: ExecutionContext) extends GnostixAPIStack with FacebookDashboardServletRoutes
