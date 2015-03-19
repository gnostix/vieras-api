package gr.gnostix.api.servlets

import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.models.pgDao.GeoLocationDao
import gr.gnostix.api.models.plainModels.{ApiMessages, CountriesLine, ErrorDataResponse}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s.{DefaultFormats, Formats}
import org.scalatra._
import org.scalatra.json.JacksonJsonSupport

import scala.concurrent.ExecutionContext

/**
 * Created by rebel on 27/11/14.
 */

trait GeoServicesRoutes extends ScalatraServlet
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

  //     /api/user/account/geolocation/services/*

  // get all data for hotel for one profile
  get("/profile/:profileId/company/:companyId/:fromDate/:toDate") {
    logger.info(s"----> get all data for hotel for  one profile " +
      s"  /api/user/account/geolocation/services/*  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt


      val rawData = GeoLocationDao.getDataByProfileId(executor, fromDate, toDate, profileId, companyId)
      new AsyncResult {
        val is =
          for {
            data <- rawData
          } yield f2(data)
      }


    } catch {
      case e: NumberFormatException => {
        ErrorDataResponse(404, "Error on data")
      }
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        ErrorDataResponse(404, "Error on data")
      }
    }
  }

  // get all data for hotel for one profile
  get("/profile/:profileId/company/:companyId/datasource/:datasourceId/:fromDate/:toDate") {
    logger.info(s"----> get all data for hotel for  one profile " +
      s"  /api/user/account/geolocation/services/*  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt

      val datasourceId = params("datasourceId").toInt

      val rawData = GeoLocationDao.getDataByDatasourceId(executor, fromDate, toDate, profileId, companyId, datasourceId)
      new AsyncResult {
        val is =
          for {
            data <- rawData
          } yield f2(data)
      }


    } catch {
      case e: NumberFormatException => {
        ErrorDataResponse(404, "Error on data")
      }
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        ErrorDataResponse(404, "Error on data")
      }
    }
  }

  // get all data for hotel for one profile
  get("/profile/:profileId/company/:companyId/account/:credId/:fromDate/:toDate") {
    logger.info(s"----> get all data for hotel for  one profile " +
      s"  /api/user/account/geolocation/services/*  ")
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

      val rawData = GeoLocationDao.getDataByCredentialsId(executor, fromDate, toDate, profileId, companyId, credId)
      new AsyncResult {
        val is =
          for {
            data <- rawData
          } yield f2(data)
      }


    } catch {
      case e: NumberFormatException => {
        ErrorDataResponse(404, "Error on data")
      }
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        ErrorDataResponse(404, "Error on data")
      }
    }
  }


  def f2(data: Option[List[CountriesLine]]) = {
    data match {
      case Some(dt: List[CountriesLine]) => ApiMessages.generalSuccess("countries", dt)
      case None => {
        logger.info(s"-----> f2 Option(None) ")
        ApiMessages.generalSuccess("countries", "")
      }
    }
  }

}

case class GeoServicesServlet(executor: ExecutionContext) extends GnostixAPIStack with GeoServicesRoutes
