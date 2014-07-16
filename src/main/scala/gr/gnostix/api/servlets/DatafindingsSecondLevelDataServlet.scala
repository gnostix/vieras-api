package gr.gnostix.api.servlets

import gr.gnostix.api.GnostixAPIStack
import org.scalatra.{CorsSupport, ScalatraServlet}
import org.scalatra.json.JacksonJsonSupport
import gr.gnostix.api.auth.AuthenticationSupport
import org.json4s.{DefaultFormats, Formats}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import gr.gnostix.api.models._
import gr.gnostix.api.models.DataResponse
import gr.gnostix.api.models.AllDataResponse

trait RestDatafindingsSecondLevelDataRoutes extends ScalatraServlet
with JacksonJsonSupport
with AuthenticationSupport
with CorsSupport {

  options("/*") {
    response.setHeader("Access-Control-Allow-Headers", request.getHeader("Access-Control-Request-Headers"))
  }

  // Sets up automatic case class to JSON output serialization, required by
  // the JValueResult trait.
  protected implicit val jsonFormats: Formats = DefaultFormats
  //val db: Database

  before() {
    contentType = formats("json")
    requireLogin()
  }


  // mount point /api/user/datafindings/raw/secondlevel/*

   get("/profile/:profileId/data/:datasource/:msgId") {
    logger.info(s"---->   second level /data/:datasource/:msgId ${params("datasource")}  ${params("msgId")}  ")
    try {

      val profileId = params("profileId").toInt
      val rawData = DatafindingsSecondLevelDataDAO.getSecondLevelData(params("msgId").toInt, params("datasource"))
      DataResponse(200, "Coulio Bro!!!", rawData)
    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
       }
    }
  }

}

case class DatafindingsSecondLevelDataServlet() extends GnostixAPIStack with RestDatafindingsSecondLevelDataRoutes
