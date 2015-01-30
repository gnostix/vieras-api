package gr.gnostix.api.servlets

import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.models.pgDao.MySocialChannelHotelDao
import gr.gnostix.api.models.plainModels.{DataResponse, ErrorDataResponse}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json.JacksonJsonSupport
import org.scalatra.{CorsSupport, ScalatraServlet}

import scala.concurrent.ExecutionContext


trait HospitalityLineCountsRoutes extends ScalatraServlet
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

  // mount point /api/user/socialchannels/hotel/line/*

  // get all data for hotel for one profile
  get("/profile/:profileId/:fromDate/:toDate") {
    logger.info(s"----> get all data for hotel for  one profile " +
      s"  /api/user/socialchannels/hotel/line/*  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt

      // "line" for line data per day , week ect..
      val rawData = MySocialChannelHotelDao.getDataCounts(fromDate, toDate, profileId, "line", None)
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


  // get all data for hotel for one profile SUM
  get("/profile/:profileId/:fromDate/:toDate/total") {
    logger.info(s"----> get all data for hotel for  one profile " +
      s"  /api/user/socialchannels/hotel/line/*  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt

      // "total" for the total sum of messages for a period
      val rawData = MySocialChannelHotelDao.getDataCounts(fromDate, toDate, profileId, "total", None)
      rawData match {
        case Some(data) => DataResponse(200, "Coulio Bro!!!", data)
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


  // get all data for hotel for one profile hotelSource = (ALL or tripadvisor id, booking id ..klp..klp..)
  get("/profile/:profileId/datasource/:datasourceId/:fromDate/:toDate") {
    logger.info(s"----> get all data for hotel for  one account datasourceId ID = (ALL, tripadvisor Id, booking Id ..klp..klp..)" +
      s"  /api/user/socialchannels/hotel/line/* ${params("datasourceId")} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val datasourceId = params("datasourceId").toInt

      val rawData = MySocialChannelHotelDao.getDataCounts(fromDate, toDate, profileId, "line", Some(datasourceId))
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

  // get all data for hotel for one profile hotelSource = (ALL or tripadvisor id, booking id ..klp..klp..)--- SUM
  get("/profile/:profileId/datasource/:datasourceId/:fromDate/:toDate/total") {
    logger.info(s"----> get all data for hotel for  one account datasourceId ID = (ALL, tripadvisor Id, booking Id ..klp..klp..)" +
      s"  /api/user/socialchannels/hotel/line/* ${params("datasourceId")} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val datasourceId = params("datasourceId").toInt

      val rawData = MySocialChannelHotelDao.getDataCounts(fromDate, toDate, profileId, "total", Some(datasourceId))
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



}


//  must do this + the count for social channel (facebook totals(post+comment) + twitter(mention+retweets))
case class HospitalityLineCountsServlet(executor: ExecutionContext) extends GnostixAPIStack with HospitalityLineCountsRoutes
