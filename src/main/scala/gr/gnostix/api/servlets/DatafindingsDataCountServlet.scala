
package gr.gnostix.api.servlets

import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.models.plainModels.{DataResponse, ErrorDataResponse, SocialDataSum}
import gr.gnostix.api.models.publicSearch.DatafindingsDataCountFutureDao
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json.JacksonJsonSupport
import org.scalatra.{AsyncResult, CorsSupport, FutureSupport}

import scala.concurrent.ExecutionContext

trait RestDatafindingsDataCountDataRoutes extends GnostixAPIStack
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

  // mount point "/api/user/datafindings/counts/*



  get("/profile/:profileId/:datasource/:fromDate/:toDate") {
    logger.info(s"---->   sentiment /sentiment/:datasource/:msgId ${params("datasource")} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt
      val rawData = DatafindingsDataCountFutureDao.getDataDefault(executor, fromDate, toDate, profileId, params("datasource"))
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

  def f2(tuple: Option[(String, Int)]) = {
    tuple match {
      case Some(x: (String, Int)) => DataResponse(200, "Data count", SocialDataSum(x._1, x._2))
      case None => ErrorDataResponse(404, "Error on data")
    }

  }

  post("/profile/:profileId/:datasource/:fromDate/:toDate/:keyortopic") {
    logger.info(s"---->   /sentiment/twitter ${params("fromDate")}  ${params("toDate")}  ")
    val idsList = parsedBody.extract[List[Int]]

    logger.info(s"----> json --> ${idsList} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt

      params("keyortopic") match {
        case "keywords" => {
          val rawData = DatafindingsDataCountFutureDao.getDataByKeywords(executor, fromDate, toDate, profileId, idsList, params("datasource"))
          new AsyncResult {
            val is =
              for {
                data <- rawData
              } yield f2(data)
          }
        }
        case "topics" => {
          val rawData = DatafindingsDataCountFutureDao.getDataByTopics(executor, fromDate, toDate, profileId, idsList, params("datasource"))
          new AsyncResult {
            val is =
              for {
                data <- rawData
              } yield f1(List(data.get))
          }
        }
      }

    } catch {
      case e: NumberFormatException => "wrong profile number"
      case e: Exception => {
        logger.info(s"-----> ${e.printStackTrace()}")
        "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
      }
    }
  }


  get("/profile/:profileId/:fromDate/:toDate/all") {

    val fromDate: DateTime = DateTime.parse(params("fromDate"),
      DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
    logger.info(s"---->   parsed date ---> ${fromDate}    ")

    val toDate: DateTime = DateTime.parse(params("toDate"),
      DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
    logger.info(s"---->   parsed date ---> ${toDate}    ")

    val profileId = params("profileId").toInt

    val dt1 = DatafindingsDataCountFutureDao.getDataDefault(executor, fromDate, toDate, profileId, "twitter")
    val dt2 = DatafindingsDataCountFutureDao.getDataDefault(executor, fromDate, toDate, profileId, "facebook")
    val dt3 = DatafindingsDataCountFutureDao.getDataDefault(executor, fromDate, toDate, profileId, "youtube")
    val dt4 = DatafindingsDataCountFutureDao.getDataDefault(executor, fromDate, toDate, profileId, "gplus")
    val dt5 = DatafindingsDataCountFutureDao.getDataDefault(executor, fromDate, toDate, profileId, "web")
    val dt6 = DatafindingsDataCountFutureDao.getDataDefault(executor, fromDate, toDate, profileId, "linkedin")
    val dt7 = DatafindingsDataCountFutureDao.getDataDefault(executor, fromDate, toDate, profileId, "news")
    val dt8 = DatafindingsDataCountFutureDao.getDataDefault(executor, fromDate, toDate, profileId, "blog")
    val dt9 = DatafindingsDataCountFutureDao.getDataDefault(executor, fromDate, toDate, profileId, "personal")
    new AsyncResult {
      val is =
        for {
          a1 <- dt1
          a2 <- dt2
          a3 <- dt3
          a4 <- dt4
          a5 <- dt5
          a6 <- dt6
          a7 <- dt7
          a8 <- dt8
          a9 <- dt9
        } yield f1(List(a1.get, a2.get, a3.get, a4.get, a5.get, a6.get, a7.get, a8.get, a9.get))
    }
  }

  def f1(allSocialDataSum: List[(String, Int)]) = {
    val mydata = allSocialDataSum.map(_._2).sum

    val s = SocialDataSum("all datasources", mydata)
    DataResponse(200, "Coulio Bro!!!", s)

  }

  post("/profile/:profileId/:fromDate/:toDate/all/:keyortopic") {
    logger.info(s"---->   /sentiment/all ${params("fromDate")}  ${params("toDate")}  ")
    val idsList = parsedBody.extract[List[Int]]

    logger.info(s"----> json --> ${idsList} ")
    try {
      val fromDate: DateTime = DateTime.parse(params("fromDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${fromDate}    ")

      val toDate: DateTime = DateTime.parse(params("toDate"),
        DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
      logger.info(s"---->   parsed date ---> ${toDate}    ")

      val profileId = params("profileId").toInt

      params("keyortopic") match {
        case "keywords" => {
          val dt1 = DatafindingsDataCountFutureDao.getDataByKeywords(executor, fromDate, toDate, profileId, idsList, "twitter")
          val dt2 = DatafindingsDataCountFutureDao.getDataByKeywords(executor, fromDate, toDate, profileId, idsList, "facebook")
          val dt3 = DatafindingsDataCountFutureDao.getDataByKeywords(executor, fromDate, toDate, profileId, idsList, "youtube")
          val dt4 = DatafindingsDataCountFutureDao.getDataByKeywords(executor, fromDate, toDate, profileId, idsList, "gplus")
          val dt5 = DatafindingsDataCountFutureDao.getDataByKeywords(executor, fromDate, toDate, profileId, idsList, "web")
          val dt6 = DatafindingsDataCountFutureDao.getDataByKeywords(executor, fromDate, toDate, profileId, idsList, "linkedin")
          val dt7 = DatafindingsDataCountFutureDao.getDataByKeywords(executor, fromDate, toDate, profileId, idsList, "news")
          val dt8 = DatafindingsDataCountFutureDao.getDataByKeywords(executor, fromDate, toDate, profileId, idsList, "blog")
          val dt9 = DatafindingsDataCountFutureDao.getDataByKeywords(executor, fromDate, toDate, profileId, idsList, "personal")
          new AsyncResult {
            val is =
              for {
                a1 <- dt1
                a2 <- dt2
                a3 <- dt3
                a4 <- dt4
                a5 <- dt5
                a6 <- dt6
                a7 <- dt7
                a8 <- dt8
                a9 <- dt9
              } yield f1(List(a1.get, a2.get, a3.get, a4.get, a5.get, a6.get, a7.get, a8.get, a9.get))
          }
        }
        case "topics" => {
          val dt1 = DatafindingsDataCountFutureDao.getDataByTopics(executor, fromDate, toDate, profileId, idsList, "twitter")
          val dt2 = DatafindingsDataCountFutureDao.getDataByTopics(executor, fromDate, toDate, profileId, idsList, "facebook")
          val dt3 = DatafindingsDataCountFutureDao.getDataByTopics(executor, fromDate, toDate, profileId, idsList, "youtube")
          val dt4 = DatafindingsDataCountFutureDao.getDataByTopics(executor, fromDate, toDate, profileId, idsList, "gplus")
          val dt5 = DatafindingsDataCountFutureDao.getDataByTopics(executor, fromDate, toDate, profileId, idsList, "web")
          val dt6 = DatafindingsDataCountFutureDao.getDataByTopics(executor, fromDate, toDate, profileId, idsList, "linkedin")
          val dt7 = DatafindingsDataCountFutureDao.getDataByTopics(executor, fromDate, toDate, profileId, idsList, "news")
          val dt8 = DatafindingsDataCountFutureDao.getDataByTopics(executor, fromDate, toDate, profileId, idsList, "blog")
          val dt9 = DatafindingsDataCountFutureDao.getDataByTopics(executor, fromDate, toDate, profileId, idsList, "personal")
          new AsyncResult {
            val is =
              for {
                a1 <- dt1
                a2 <- dt2
                a3 <- dt3
                a4 <- dt4
                a5 <- dt5
                a6 <- dt6
                a7 <- dt7
                a8 <- dt8
                a9 <- dt9
              } yield f1(List(a1.get, a2.get, a3.get, a4.get, a5.get, a6.get, a7.get, a8.get, a9.get))
          }
        }
        case _ => None
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

case class DatafindingsDataCountServlet(executor: ExecutionContext) extends GnostixAPIStack with RestDatafindingsDataCountDataRoutes


