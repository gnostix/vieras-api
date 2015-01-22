package gr.gnostix.api.servlets

import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.models.oraDao.MySocialChannelHotelDao
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
    //requireLogin()
  }

  // mount point /api/user/socialchannels/dashboard/hotel/*

  // get all data for youtube for one profile datatype
  get("/profile/:profileId/stats/:fromDate/:toDate") {
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

      val rawDataStats = MySocialChannelHotelDao.getReviewStats(executor, fromDate, toDate, profileId, None)

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
  get("/profile/:profileId/stats/datasourceid/:dsid/:fromDate/:toDate") {
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
      val dsId = params("dsid").toInt

      val rawDataStats = MySocialChannelHotelDao.getReviewStats(executor, fromDate, toDate, profileId, Some(dsId))

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


  // get all data for youtube for one profile datatype
  get("/profile/:profileId/services/:fromDate/:toDate") {
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

      val rawDataStats = MySocialChannelHotelDao.getReviewRatingStats(executor, fromDate, toDate, profileId, None)

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
  get("/profile/:profileId/services/datasourceid/:dsid/:fromDate/:toDate") {
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
      val dsId = params("dsid").toInt

      val rawDataStats = MySocialChannelHotelDao.getReviewRatingStats(executor, fromDate, toDate, profileId, Some(dsId))

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


