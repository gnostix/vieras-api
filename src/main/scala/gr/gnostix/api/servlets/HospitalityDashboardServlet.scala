package gr.gnostix.api.servlets

import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.models.pgDao.MySocialChannelHotelDao
import gr.gnostix.api.models.plainModels.{ApiData, ApiMessages, ErrorDataResponse}
import gr.gnostix.api.utilities.HelperFunctions
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json.JacksonJsonSupport
import org.scalatra.{AsyncResult, CorsSupport, FutureSupport, ScalatraServlet}

import scala.concurrent.ExecutionContext

/**
 * Created by rebel on 13/1/15.
 */

trait HospitalityDashboardServletRoutes extends ScalatraServlet
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

  // mount point /api/user/socialchannels/dashboard/hotel/*

  // get all data for youtube for one profile datatype
  get("/profile/:profileId/company/:companyId/stats/:fromDate/:toDate") {
    logger.info(s"----> get stats  one account " +
      s"  /api/user/socialchannels/dashboard/hotel/*  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt


      val rawDataStats = MySocialChannelHotelDao.getReviewStats(executor, fromDate, toDate, profileId, companyId, None)

      new AsyncResult() {
        override val is =
          for {
            a1 <- rawDataStats
          } yield HelperFunctions.f3(Some(a1.get))
      }

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }

  // get all data for youtube for one profile datatype
  get("/profile/:profileId/company/:companyId/stats/datasourceid/:dsid/:fromDate/:toDate") {
    logger.info(s"----> get stats  one account " +
      s"  /api/user/socialchannels/dashboard/hotel/*  " + params("dsid"))
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt

      val dsId = params("dsid").toInt

      val rawDataStats = MySocialChannelHotelDao.getReviewStats(executor, fromDate, toDate, profileId, companyId, Some(dsId))

      new AsyncResult() {
        override val is =
          for {
            a1 <- rawDataStats
          } yield HelperFunctions.f3(Some(a1.get))
      }

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }


  // get all data for youtube for one profile datatype
  get("/profile/:profileId/company/:companyId/sentiment/:sentiment/:fromDate/:toDate") {
    logger.info(s"----> get stats  one account " +
      s"  /api/user/socialchannels/dashboard/hotel/*  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt
      val sentiment = params("sentiment")


      val rawDataStats = MySocialChannelHotelDao.getSentimentTextData(executor, fromDate, toDate, profileId, companyId, None, sentiment)

      new AsyncResult() {
        override val is =
          for {
            a1 <- rawDataStats
          } yield HelperFunctions.f2(Some(a1.get))
      }

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }

  // get all data for youtube for one profile datatype
  get("/profile/:profileId/company/:companyId/datasourceid/:dsid/sentiment/:sentiment/:fromDate/:toDate") {
    logger.info(s"----> get stats  one account " +
      s"  /api/user/socialchannels/dashboard/hotel/*  " + params("dsid"))
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt
      val sentiment = params("sentiment")
      val dsId = params("dsid").toInt

      val rawDataStats = MySocialChannelHotelDao.getSentimentTextData(executor, fromDate, toDate, profileId, companyId, Some(dsId), sentiment)

      new AsyncResult() {
        override val is =
          for {
            a1 <- rawDataStats
          } yield HelperFunctions.f2(Some(a1.get))
      }

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }



  // get all data for youtube for one profile datatype
  get("/profile/:profileId/company/:companyId/staytype/:staytype/:fromDate/:toDate") {
    logger.info(s"----> get stats  one account " +
      s"  /api/user/socialchannels/dashboard/hotel/*  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt
      val staytype = params("staytype")


      val rawDataStats = MySocialChannelHotelDao.getStayTypeTextData(executor, fromDate, toDate, profileId, companyId, None, staytype)

      new AsyncResult() {
        override val is =
          for {
            a1 <- rawDataStats
          } yield HelperFunctions.f2(Some(a1.get))
      }

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }

  // get all data for youtube for one profile datatype
  get("/profile/:profileId/company/:companyId/datasourceid/:dsid/staytype/:staytype/:fromDate/:toDate") {
    logger.info(s"----> get stats  one account " +
      s"  /api/user/socialchannels/dashboard/hotel/*  " + params("dsid"))
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt
      val staytype = params("staytype")
      val dsId = params("dsid").toInt

      val rawDataStats = MySocialChannelHotelDao.getStayTypeTextData(executor, fromDate, toDate, profileId, companyId, Some(dsId), staytype)

      new AsyncResult() {
        override val is =
          for {
            a1 <- rawDataStats
          } yield HelperFunctions.f2(Some(a1.get))
      }

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }



  // get all data for youtube for one profile datatype
  get("/profile/:profileId/company/:companyId/service/:service/sentiment/:sentiment/:fromDate/:toDate") {
    logger.info(s"----> get stats  one account " +
      s"  /api/user/socialchannels/dashboard/hotel/*  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt
      val service = params("service")
      val sentiment = params("sentiment")


      val rawDataStats = MySocialChannelHotelDao.getServiceBySentimentTextData(executor, fromDate, toDate, profileId, companyId, None, service, sentiment)

      new AsyncResult() {
        override val is =
          for {
            a1 <- rawDataStats
          } yield HelperFunctions.f2(Some(a1.get))
      }

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }

  // get all data for youtube for one profile datatype
  get("/profile/:profileId/company/:companyId/datasourceid/:dsid/service/:service/sentiment/:sentiment/:fromDate/:toDate") {
    logger.info(s"----> get stats  one account " +
      s"  /api/user/socialchannels/dashboard/hotel/*  " + params("dsid"))
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt
      val service = params("service")
      val sentiment = params("sentiment")
      val dsId = params("dsid").toInt

      val rawDataStats = MySocialChannelHotelDao.getServiceBySentimentTextData(executor, fromDate, toDate, profileId, companyId, Some(dsId), service, sentiment)

      new AsyncResult() {
        override val is =
          for {
            a1 <- rawDataStats
          } yield HelperFunctions.f2(Some(a1.get))
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
          case _ => ApiMessages.generalSuccessOneParam(Map(dt.dataName -> dt.data))
        }

        hasData
      }
      case None => ErrorDataResponse(404, "Error on data")
    }

  }



  get("/profile/:profileId/company/:companyId/services/:fromDate/:toDate") {
    logger.info(s"----> get stats  one account " +
      s"  /api/user/socialchannels/dashboard/hotel/*  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt


      val rawDataStats = MySocialChannelHotelDao.getReviewRatingStats(executor, fromDate, toDate, profileId, companyId, None)

      new AsyncResult() {
        override val is =
          for {
            a1 <- rawDataStats
          } yield HelperFunctions.f3(Some(a1.get))
      }

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }


  get("/profile/:profileId/company/:companyId/services/datasourceid/:dsid/:fromDate/:toDate") {
    logger.info(s"----> get stats  one account " +
      s"  /api/user/socialchannels/dashboard/hotel/*  " + params("dsid"))
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt

      val dsId = params("dsid").toInt

      val rawDataStats = MySocialChannelHotelDao.getReviewRatingStats(executor, fromDate, toDate, profileId, companyId, Some(dsId))

      new AsyncResult() {
        override val is =
          for {
            a1 <- rawDataStats
          } yield HelperFunctions.f3(Some(a1.get))
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

case class HospitalityDashboardServlet(executor: ExecutionContext) extends GnostixAPIStack with HospitalityDashboardServletRoutes


