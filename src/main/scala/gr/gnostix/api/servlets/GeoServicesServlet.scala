package gr.gnostix.api.servlets

import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.models.{ApiMessages, FutureSentimentDao, ErrorDataResponse, DataResponse}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.{CorsSupport, ScalatraServlet}
import org.scalatra.json.JacksonJsonSupport

import scala.concurrent.ExecutionContext

/**
 * Created by rebel on 27/11/14.
 */

trait GeoServicesRoutes extends ScalatraServlet
with JacksonJsonSupport
with AuthenticationSupport
with CorsSupport {


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

  //     /api/user/account/geolocation/services/*

  // get all data for hotel for one profile
  get("/profile/:profileId/:fromDate/:toDate") {
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

      // "line" for line data per day , week ect..
      val rawData = Some("koko") //MyS  (fromDate, toDate, profileId, "line", None)

      rawData match {
        case Some(data) =>  ApiMessages.generalSuccess("koko", data)
        case _ => ErrorDataResponse(404, "Error on data")
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

case class GeoServicesServlet(executor: ExecutionContext) extends GnostixAPIStack with GeoServicesRoutes
