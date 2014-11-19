package gr.gnostix.api.servlets

import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.models._
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

  get("/profile/:profileId/:service/:fromDate/:toDate") {
    logger.info(s"---->   sentiment /sentiment/:service/ ${params("service")} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val rawData = HospitalityServicesDao.getByName(executor, params("service"), profileId, fromDate, toDate)
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

  def f2(data: Option[HospitalityServicesSentiment]) = {
    data match {
      case Some(x: HospitalityServicesSentiment) => Map("status" -> 200, "message" -> "all good", "payload" -> x)
      case None => ErrorDataResponse(404, "Error on data")
    }
  }

  get("/profile/:profileId/:fromDate/:toDate") {

    logger.info(s"---->   sentiment /sentiment/services ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt

      // hotel services to get
      val hotelStaff = HospitalityServicesDao.getByName(executor, "Staff", profileId, fromDate, toDate)
      val hotelRooms = HospitalityServicesDao.getByName(executor, "Rooms", profileId, fromDate, toDate)
      val hotelValue = HospitalityServicesDao.getByName(executor, "Value", profileId, fromDate, toDate)
      val hotelCleanliness = HospitalityServicesDao.getByName(executor, "Cleanliness", profileId, fromDate, toDate)
      val hotelLocation = HospitalityServicesDao.getByName(executor, "Location", profileId, fromDate, toDate)

      new AsyncResult {
        val is =
          for {
            data1 <- hotelStaff
            data2 <- hotelRooms
            data3 <- hotelValue
            data4 <- hotelCleanliness
            data5 <- hotelLocation

          } yield f3(List(data1.get, data2.get, data3.get,
            data4.get, data5.get))
      }
    } catch {
      case e: NumberFormatException => ErrorDataResponse(404, "wrong profile number")
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        ErrorDataResponse(404, "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss")
      }
    }
  }

  def f3(data: List[HospitalityServicesSentiment]) = {
    val sent = ArrayBuffer[HospitalityServicesSentiment]()
    for (a <- data) {
      a match {
        case s: HospitalityServicesSentiment => sent.+=(s)
        case _ => logger.info(s"-----> None => do nothing..}")
      }
    }
    Map("status" -> 200, "message" -> "all good", "payload" -> sent)
    //ErrorDataResponse(404, "Error on data")
  }

}

case class HospitalityServicesServlet(executor: ExecutionContext) extends GnostixAPIStack with ServicesApiRoutes
