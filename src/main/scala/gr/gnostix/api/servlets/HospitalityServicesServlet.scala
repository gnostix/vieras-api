package gr.gnostix.api.servlets

import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.models.pgDao.{HospitalityServicesDao, HospitalityServicesSentiment}
import gr.gnostix.api.models.plainModels.{ApiData, ApiMessages, ErrorDataResponse}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s.{DefaultFormats, Formats}
import org.scalatra._
import org.scalatra.json.JacksonJsonSupport

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

/**
 * Created by rebel on 18/11/14.
 */

trait ServicesApiRoutes
  extends ScalatraServlet
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


  //mount point /api/user/account/hospitality/services/*

  get("/profile/:profileId/company/:companyId/:service/:fromDate/:toDate") {
    logger.info(s"---->   sentiment /sentiment/:service/ ${params("service")} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt

      val rawData = HospitalityServicesDao.getDataServiceByName(executor, params("service"), user.userId, profileId,
        companyId, fromDate, toDate)
      new AsyncResult {
        val is =
          for {
            data <- rawData
          } yield f2(data)
      }
    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }



  get("/profile/:profileId/company/:companyId/:fromDate/:toDate") {

    logger.info(s"---->   sentiment /sentiment/services ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt


      // hotel services to get
      val servicesStats = HospitalityServicesDao.getReviewRatingStats(executor, fromDate, toDate, user.userId, profileId, companyId)

      new AsyncResult {
        val is =
          for {
            data <- servicesStats

          } yield f2(data)
      }
    } catch {
      case e: NumberFormatException => ErrorDataResponse(404, "wrong profile number")
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        ErrorDataResponse(404, "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss")
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

}

case class HospitalityServicesServlet(executor: ExecutionContext) extends GnostixAPIStack with ServicesApiRoutes
