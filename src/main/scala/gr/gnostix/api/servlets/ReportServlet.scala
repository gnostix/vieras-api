package gr.gnostix.api.servlets

import java.io.{OutputStream, FileInputStream, File}
import java.sql.Date

import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.models.pgDao.UserDao
import gr.gnostix.api.utilities.Reporting
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json.JacksonJsonSupport
import org.scalatra.{CorsSupport, FutureSupport, ScalatraServlet}

import scala.concurrent.ExecutionContext


trait ReportServletRoutes extends ScalatraServlet
with JacksonJsonSupport
with AuthenticationSupport
with CorsSupport
with FutureSupport {
  options("/*") {
    response.setHeader("Access-Control-Allow-Headers", request.getHeader("Access-Control-Request-Headers"))
  }

  protected implicit val jsonFormats: Formats = DefaultFormats

  before() {
    //contentType = formats("json")
    //contentType = "application/octet-stream"
    //requireLogin()
  }

  // mount point /api/user/dashboard/report/*

  get("/rp") {
    try {
//            val path = servletContext.getRealPath("/")
//            logger.info(s"-------------->   " + path)
//            val file = new java.io.File(path + "/api/reports/CV_AlexPappas-January-2014.doc")
//            contentType = "application/octet-stream"
//            response.setHeader("Content-Disposition", "attachment; filename=" + file.getName)
//
//            response.redirect("/api/reports/" + file.getName)

      Map("usersNum5" -> UserDao.getUsers)

    } catch {
      case e: Exception => e.printStackTrace()
    }

  }

  // get all data for youtube for one profile datatype
  get("/profile/:profileId/:fromDate/:toDate") {
    logger.info(s"---->  " +
      s"  /api/user/dashboard/report/*  ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt

      val path = servletContext.getRealPath("/") + "/reports"
      val reporting: Reporting = new Reporting
      val fileReport = reporting.generateReport(new java.util.Date(), new java.util.Date(), "docx", path)

      logger.info(s"-------------->   " + path)
      val file = new java.io.File(path + fileReport)
      contentType = "application/octet-stream"
      response.setHeader("Content-Disposition", "attachment; filename=" + file.getName)
      response.setHeader("Access-Control-Allow-Headers", request.getHeader("Access-Control-Request-Headers"))
      response.redirect("/reports/" + file.getName)
    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }

}


case class ReportServlet(executor: ExecutionContext) extends GnostixAPIStack with ReportServletRoutes


