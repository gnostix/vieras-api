package gr.gnostix.api.servlets

/**
 * Created by rebel on 1/7/15.
 */
 
  import gr.gnostix.api.GnostixAPIStack
  import org.scalatra.{FutureSupport, AsyncResult, CorsSupport, ScalatraServlet}
  import org.scalatra.json.JacksonJsonSupport
  import gr.gnostix.api.auth.AuthenticationSupport
  import org.json4s.{DefaultFormats, Formats}
  import org.joda.time.DateTime
  import org.joda.time.format.DateTimeFormat
  import gr.gnostix.api.models._

  import scala.concurrent.{Future, ExecutionContext}
  import scala.util.{Failure, Success}

  trait RestSocialChannelsYtLineDataRoutes extends GnostixAPIStack
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
      //requireLogin()
    }

    // mount point /api/user/socialchannels/youtube/line/*

    get("/profile/:profileId/:fromDate/:toDate") {
      logger.info(s"----> get all data for youtube for  one account " +
        s"  /api/user/socialchannels/youtube/line/*  ")
      try {
        val fromDate: DateTime = DateTime.parse(params("fromDate"),
          DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
        logger.info(s"---->   parsed date ---> ${fromDate}    ")

        val toDate: DateTime = DateTime.parse(params("toDate"),
          DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
        logger.info(s"---->   parsed date ---> ${toDate}    ")

        val profileId = params("profileId").toInt

        val rawData = MySocialChannelDaoYt.getLineCounts(executor, fromDate, toDate, profileId,  None)

        new AsyncResult() {
          override val is =
            for {
              a1 <- rawData
            } yield f2(a1)
        }


      } catch {
        case e: NumberFormatException => "wrong profile number"
        case e: Exception => {
          logger.info(s"-----> ${e.printStackTrace()}")
          "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
        }
      }
    }

     get("/profile/:profileId/:credId/:fromDate/:toDate") {
      logger.info(s"----> get all data for youtube for  one account  " +
        s"  /api/user/socialchannels/youtube/line/*   ")
      try {
        val fromDate: DateTime = DateTime.parse(params("fromDate"),
          DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
        logger.info(s"---->   parsed date ---> ${fromDate}    ")

        val toDate: DateTime = DateTime.parse(params("toDate"),
          DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
        logger.info(s"---->   parsed date ---> ${toDate}    ")

        val profileId = params("profileId").toInt
        val credId = params("credId").toInt

        val rawData = MySocialChannelDaoYt.getLineCounts(executor, fromDate, toDate, profileId, Some(credId))

        new AsyncResult() {
          override val is =
            for {
              a1 <- rawData
            } yield f2(a1)
        }


      } catch {
        case e: NumberFormatException => "wrong profile number"
        case e: Exception => {
          logger.info(s"-----> ${e.printStackTrace()}")
          "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
        }
      }
    }


    def f2(dashboardData: Option[ApiData]) = {
      dashboardData match {
        case Some(dt) => {

          val hasData = dt.dataName match {
            case "nodata" => ApiMessages.generalSuccessNoData
            case _ => ApiMessages.generalSuccessOneParam(dt)
          }

          hasData
        }
        case None => ErrorDataResponse(404, "Error on data")
      }

    }
  }

  case class SocialChannelsYoutubeLineServlet(executor: ExecutionContext) extends GnostixAPIStack with RestSocialChannelsYtLineDataRoutes


