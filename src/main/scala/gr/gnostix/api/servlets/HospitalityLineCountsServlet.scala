package gr.gnostix.api.servlets

import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.models.pgDao.MySocialChannelHotelDao
import gr.gnostix.api.models.plainModels.{DataResponse, ErrorDataResponse}
import gr.gnostix.api.utilities.HelperFunctions
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json.JacksonJsonSupport
import org.scalatra.{FutureSupport, AsyncResult, CorsSupport, ScalatraServlet}

import scala.concurrent.ExecutionContext


trait HospitalityLineCountsRoutes extends ScalatraServlet
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

  // mount point /api/user/socialchannels/hotel/line/*

  // get all data for hotel for one profile
  get("/profile/:profileId/company/:companyId/:fromDate/:toDate") {
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
      val companyId = params("companyId").toInt

      // "line" for line data per day , week ect..
      val rawData = MySocialChannelHotelDao.getDataCounts(fromDate, toDate, profileId, companyId, "line", None)
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
  get("/profile/:profileId/company/:companyId/:fromDate/:toDate/total") {
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
      val companyId = params("companyId").toInt

      // "total" for the total sum of messages for a period
      val rawData = MySocialChannelHotelDao.getDataCounts(fromDate, toDate, profileId, companyId, "total", None)
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
  get("/profile/:profileId/company/:companyId/datasource/:datasourceId/:fromDate/:toDate") {
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
      val companyId = params("companyId").toInt
      val datasourceId = params("datasourceId").toInt

      val rawData = MySocialChannelHotelDao.getDataCounts(fromDate, toDate, profileId, companyId, "line", Some(datasourceId))
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
  get("/profile/:profileId/company/:companyId/datasource/:datasourceId/:fromDate/:toDate/total") {
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
      val companyId = params("companyId").toInt
      val datasourceId = params("datasourceId").toInt

      val rawData = MySocialChannelHotelDao.getDataCounts(fromDate, toDate, profileId, companyId, "total", Some(datasourceId))
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

  // get all data for hotel for one profile
  get("/profile/:profileId/company/:companyId/services/sentiment/:fromDate/:toDate") {
    logger.info(s"----> get services sentiment for hotel for  one profile " +
      s"  /api/user/socialchannels/hotel/line/*  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val companyId = params("companyId").toInt

      // "line" for line data per day , week ect..
      val rawData = MySocialChannelHotelDao.getServicesLineCountsAverageSentiment(executor, fromDate, toDate, profileId, companyId, None)
      new AsyncResult() {
        override val is =
          for {
            a1 <- rawData
          } yield HelperFunctions.f2(a1)
      }

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }

  // get all data for hotel for one profile
  get("/profile/:profileId/company/:companyId/services/sentiment/datasource/:datasourceId/:fromDate/:toDate") {
    logger.info(s"----> get services sentiment for hotel for  one profile " +
      s"  /api/user/socialchannels/hotel/line/*  ")
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

      // "line" for line data per day , week ect..
      val rawData = MySocialChannelHotelDao.getServicesLineCountsAverageSentiment(executor, fromDate, toDate, profileId, companyId, Some(datasourceId))
      new AsyncResult() {
        override val is =
          for {
            a1 <- rawData
          } yield HelperFunctions.f2(a1)
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
