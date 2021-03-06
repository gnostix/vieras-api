
  import gr.gnostix.api.GnostixAPIStack
  import gr.gnostix.api.auth.AuthenticationSupport
  import gr.gnostix.api.models.pgDao.MySocialChannelDaoTw
  import gr.gnostix.api.models.plainModels._
  import org.joda.time.DateTime
  import org.joda.time.format.DateTimeFormat
  import org.json4s.{DefaultFormats, Formats}
  import org.scalatra.json.JacksonJsonSupport
  import org.scalatra.{AsyncResult, CorsSupport, FutureSupport}

import scala.concurrent.ExecutionContext

  trait RestSocialChannelsTwLineDataRoutes extends GnostixAPIStack
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

    // mount point /api/user/socialchannels/twitter/line/*

    // get all data for twitter for one profile datatype = (post or comment)
    get("/profile/:profileId/company/:companyId/:dataType/:fromDate/:toDate") {
      logger.info(s"----> get all data for twitter for  one account datatype = (mention , retweet)" +
        s"  /api/user/socialchannels/twitter/line/* ${params("dataType")} ")
      try {
        val fromDate: DateTime = DateTime.parse(params("fromDate"),
          DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
        logger.info(s"---->   parsed date ---> ${fromDate}    ")

        val toDate: DateTime = DateTime.parse(params("toDate"),
          DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
        logger.info(s"---->   parsed date ---> ${toDate}    ")

        val profileId = params("profileId").toInt
        val companyId = params("companyId").toInt

        val rawData = MySocialChannelDaoTw.getLineCounts(fromDate, toDate, user.userId, profileId, companyId, params("dataType"), None)
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

    // get all data for twitter for one account datatype = (post or comment)
    get("/profile/:profileId/company/:companyId/:dataType/:credId/:fromDate/:toDate") {
      logger.info(s"----> get all data for twitter for  one account datatype = (mention , retweet) " +
        s"  /api/user/socialchannels/twitter/line/*  ${params("dataType")} ")
      try {
        val fromDate: DateTime = DateTime.parse(params("fromDate"),
          DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
        logger.info(s"---->   parsed date ---> ${fromDate}    ")

        val toDate: DateTime = DateTime.parse(params("toDate"),
          DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
        logger.info(s"---->   parsed date ---> ${toDate}    ")

        val profileId = params("profileId").toInt
        val companyId = params("companyId").toInt
        val credId = params("credId").toInt

        val rawData = MySocialChannelDaoTw.getLineCounts(fromDate, toDate, user.userId, profileId, companyId, params("dataType"), Some(credId))
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

    // get all data for twitter for  all accounts datatype = (all, post, comment)
    get("/profile/:profileId/company/:companyId/:fromDate/:toDate/all") {
      logger.info(s"---->   /api/user/socialchannels/twitter/line/* ${params("profileId")} ")
      try {
        val fromDate: DateTime = DateTime.parse(params("fromDate"),
          DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
        logger.info(s"---->   parsed date ---> ${fromDate}    ")

        val toDate: DateTime = DateTime.parse(params("toDate"),
          DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
        logger.info(s"---->   parsed date ---> ${toDate}    ")

        val profileId = params("profileId").toInt
        val companyId = params("companyId").toInt


        val mention = MySocialChannelDaoTw.getLineAllData(executor, fromDate, toDate, user.userId, profileId, companyId, "mention", None)
        val retweet = MySocialChannelDaoTw.getLineAllData(executor, fromDate, toDate, user.userId, profileId, companyId, "retweet", None)

        val theData =
          new AsyncResult() {
            override val is =
              for {
                a1 <- mention
                a2 <- retweet
              } yield f1(List(a1.get, a2.get))
          }
        //return the data

        theData
      } catch {
        case e: NumberFormatException => "wrong profile number"
        case e: Exception => {
          logger.info(s"-----> ${e.printStackTrace()}")
          "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
        }
      }
    }


    def f1(allSocialData: List[SocialData]) = {
      val mydata = allSocialData.map(_.data).flatten

      val k = (allSocialData.map(_.data).flatten).groupBy(_.asInstanceOf[DataLineGraph].msgDate).map {
        case (key, msgList) => (key, msgList.map(_.asInstanceOf[DataLineGraph].msgNum).sum)
      }.map {
        case (x, y) => new DataLineGraph(y, x)
      }
      logger.info(s"-----> kkkkkkkkk ${k}")

      val theData = k.toList
      if(theData.size > 0)
        ApiMessages.generalSuccess("data", theData.sortBy(_.msgDate.getTime) )
      else
        ApiMessages.generalSuccess("data", theData )


    }

    // get SUM data for twitter for  all accounts datatype = (all, post, comment)
    get("/profile/:profileId/company/:companyId/:fromDate/:toDate/total/all") {
      logger.info(s"---->   /api/user/socialchannels/twitter/line/* ${params("profileId")} ")

      try {
        val fromDate: DateTime = DateTime.parse(params("fromDate"),
          DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
        logger.info(s"---->   parsed date ---> ${fromDate}    ")

        val toDate: DateTime = DateTime.parse(params("toDate"),
          DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
        logger.info(s"---->   parsed date ---> ${toDate}    ")

        val profileId = params("profileId").toInt
        val companyId = params("companyId").toInt

        val mention = MySocialChannelDaoTw.getTotalSumData(executor, fromDate, toDate, user.userId, profileId, companyId, "totalmention", None)
        val retweet = MySocialChannelDaoTw.getTotalSumData(executor, fromDate, toDate, user.userId, profileId, companyId, "totalretweet", None)

        val theData =
          new AsyncResult() {
            override val is =
              for {
                a1 <- mention
                a2 <- retweet
              //  } yield (a1.get, a2.get)
              } yield f2(List(a1.get, a2.get))
          }

        //return the data
        theData
      } catch {
        case e: NumberFormatException => "wrong profile number"
        case e: Exception => {
          logger.info(s"-----> ${e.printStackTrace()}")
          "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
        }
      }
    }

    def f2(a: List[ApiData]) = {
      val theSum = a.groupBy(_.dataName).map {
        case (x, y) => (x, y.map(x => x.data.asInstanceOf[Int]).sum)
      }

      Map("status" -> 200, "message" -> "Coulio Bro!!!", "payload" -> theSum)
    }

    // get all data for twitter for  one account datatype = (all, post, comment)
    get("/profile/:profileId/company/:companyId/:engId/:fromDate/:toDate/total/all") {
      logger.info(s"---->   /api/user/socialchannels/twitter/line/* ${params("engId")} ")
      try {
        val fromDate: DateTime = DateTime.parse(params("fromDate"),
          DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
        logger.info(s"---->   parsed date ---> ${fromDate}    ")

        val toDate: DateTime = DateTime.parse(params("toDate"),
          DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
        logger.info(s"---->   parsed date ---> ${toDate}    ")

        val profileId = params("profileId").toInt
        val companyId = params("companyId").toInt
        val engId = params("engId").toInt

        val mention = MySocialChannelDaoTw.getTotalSumData(executor, fromDate, toDate, user.userId, profileId, companyId, "totalmention", Some(engId))
        val retweet = MySocialChannelDaoTw.getTotalSumData(executor, fromDate, toDate, user.userId, profileId, companyId, "totalretweet", Some(engId))

        val theData =
          new AsyncResult() {
            override val is =
              for {
                a1 <- mention
                a2 <- retweet
              } yield f2(List(a1.get, a2.get))
          }

        //return the data
        theData
      } catch {
        case e: NumberFormatException => "wrong profile number"
        case e: Exception => {
          logger.info(s"-----> ${e.printStackTrace()}")
          "Wrong Date format. You should sen in format dd-MM-yyyy HH:mm:ss "
        }
      }
    }


  }

  case class SocialChannelsTwitterLineServlet(executor: ExecutionContext) extends GnostixAPIStack with RestSocialChannelsTwLineDataRoutes

