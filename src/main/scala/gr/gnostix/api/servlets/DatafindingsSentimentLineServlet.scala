

  package gr.gnostix.api.servlets

  import gr.gnostix.api.GnostixAPIStack
  import org.scalatra.{CorsSupport, ScalatraServlet}
  import org.scalatra.json.JacksonJsonSupport
  import gr.gnostix.api.auth.AuthenticationSupport
  import org.json4s.{DefaultFormats, Formats}
  import org.joda.time.DateTime
  import org.joda.time.format.DateTimeFormat
  import gr.gnostix.api.models._

  trait RestDatafindingsSentimentLineDataRoutes extends ScalatraServlet
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


    // mount point "/api/user/datafindings/sentiment/*




    get("/profile/:profileId/:fromDate/:toDate/all") {
      logger.info(s"---->   sentiment /sentiment/:datasource/:msgId ${params("datasource")} ")
      try {
        val fromDate: DateTime = DateTime.parse(params("fromDate"),
          DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
        logger.info(s"---->   parsed date ---> ${fromDate}    ")

        val toDate: DateTime = DateTime.parse(params("toDate"),
          DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
        logger.info(s"---->   parsed date ---> ${toDate}    ")

        val profileId = params("profileId").toInt

        SocialDatasources.myDatasources

        val rawData = DatafindingsSentimentLineDao.getDataDefault(fromDate, toDate, profileId, params("datasource"))
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
        val rawData = DatafindingsSentimentLineDao.getDataDefault(fromDate, toDate, profileId, params("datasource"))
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

        val rawData = params("keyortopic") match {
          case "keywords" => DatafindingsSentimentLineDao.getDataByKeywords(fromDate, toDate, profileId, idsList, params("datasource"))
          case "topics" => DatafindingsSentimentLineDao.getDataByTopics(fromDate, toDate, profileId, idsList, params("datasource"))
        }

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

  case class DatafindingsSentimentLineServlet() extends GnostixAPIStack with RestDatafindingsSentimentLineDataRoutes

