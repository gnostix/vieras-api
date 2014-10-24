package gr.gnostix.api.servlets

import gr.gnostix.api.db.lifted.OracleLiftedTables
import gr.gnostix.api.utilities.{TwOauth, FbExtendedToken}
import org.scalatra._
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json._
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.models._
import twitter4j.Twitter
import twitter4j.auth.RequestToken

import scala.concurrent.{Future, ExecutionContext}
import scala.util.{Try, Failure, Success}
import scala.collection.JavaConversions._


trait ConfigApiRoutes extends ScalatraServlet
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
    //requireLogin()
  }


  //mount point /api/user/account/*


  post("/fb/pages/:token") {
     val fbToken = parsedBody.extract[FacebookToken]
    logger.info("---->   GET FB TOKEN !!!!    ")
    //val token = Map("token" -> FbExtendedToken.getExtendedToken(params("token")))
    val token = FbExtendedToken.getExtendedToken(fbToken.token)
    val pages = FbExtendedToken.getUserPages(token.getAccessToken)
    val data = FacebookPageAuth(token.getAccessToken, token.getExpires, pages.toList)
    DataResponse(200, "All good", data)
  }

  //test route
  get("/fb/pages") {
    logger.info("---->   GET FB PAGES !!!!    ")
    val pages = FbExtendedToken.getUserPages("CAACfbmDZBqF0BAFvLIViNIZBuZCQESer8kEGUJMoDYWlyLmd492pCBcdKYmNPZC8yWNviiZA2" +
      "kBnJhtE3P0ku87Y8zBe8skxbLiuxYMmxquZAM61VBwFDfLlbPhUjTIAhFWi7YALe1BsY4eUz5FH068Uy6wCmqZAZCbKyQ98Chwr7B2w03jQIGkX")
    pages.toList
  }



  get("/tw/auth/:pin") {
    logger.info("---->   Twitter PIN !!!!    ")
    TwOauth.getUserToken(params("pin"))
  }

  get("/tw/auth") {
    logger.info("---->   Twitter AUTH!!!!    ")
    TwOauth.getUrlAuth
  }

  get("/tw/add/proc"){
    SocialAccountsTwitterDao.addAccount();
  }


  get("/profiles/usage") {
    logger.info("---->   return all profiles/usage with userlevel id     ")
    try {
      ProfilesUsage.findByUserlevel(user.userLevel) //user level....!!!
    } catch {
      case e: Exception => "Something went wrong"
    }
  }

  get("/profiles/all") {
    logger.info("---->   return all profiles with id and name     ")
    ProfileDao.getAllProfiles(user.userId)
  }

  get("/profile/:id") {
    logger.info(s"---->   return profile with id ${params("id")}     ")
    ProfileDao.findById(params("id").toInt, user.userId)
  }



  // TOPICS
  get("/profile/:profileId/topics/all") {
    logger.info(s"---->   return all the topics for this profileId ${params("profileId")}     ")
    TopicDao.getAllTopics(params("profileId").toInt, user.userId)
  }


  get("/profile/:profileId/topic/:id") {
    logger.info(s"---->   return the topic id for this profileId ${params("id")}     ")
    TopicDao.findById(params("id").toInt, params("profileId").toInt, user.userId)
  }

  post("/profile/:profileId/topic") {
    logger.info(s"---->   add a new topic for this profileId ${params("profileId")}     ")
    val myTopics = parsedBody.extract[List[Topic]]
    logger.info(s"---->   add a new topic ${myTopics.size} ")
    if (myTopics.size > 0)
      myTopics.foreach(TopicDao.addTopic)
    else
      "404 wrong data for this topic"
  }

  put("/profile/:profileId/topic") {
    logger.info(s"---->   update topic for this profileId ${params("profileId")}     ")
    val myTopics = parsedBody.extract[List[Topic]]
    logger.info(s"---->  update keyword ${myTopics.size}    ")
    myTopics.foreach(TopicDao.updateTopic(_, params("profileId").toInt, user.userId))
  }

  delete("/profile/:profileId/topic") {
    logger.info(s"---->   delete list of topic(s) for this profileId ${params("profileId")}     ")
    val topicIds = parsedBody.extract[List[Int]]
    logger.info(s"---->   delete list of topic(s) ${topicIds.size}     ")
    if (topicIds.size > 0)
      TopicDao.deleteTopics(topicIds, params("profileId").toInt, user.userId)
  }



  // KEYWORDS

  get("/profile/:profileId/topic/:topicId/keywords/all") {
    logger.info(s"---->   return all the keywords for this topicID ${params("topicId")}     ")
    KeywordDao.getAllKeywords(params("topicId").toInt, params("profileId").toInt, user.userId)
  }


  get("/profile/:profileId/topic/:topicId/keyword/:id") {
    logger.info(s"---->   return all the keywords for this topicID ${params("topicId")}     ")
    KeywordDao.findById(params("id").toInt, params("topicId").toInt, params("profileId").toInt, user.userId)
  }

  post("/profile/:profileId/topic/:topicId/keyword") {
    logger.info(s"---->   add a new keyword for this topicID ${params("topicId")}     ")
    val mykeywords = parsedBody.extract[List[Keyword]]
    logger.info(s"---->   add a new keyword ${mykeywords.size}    ")
    mykeywords.foreach(KeywordDao.addKeyword)
  }

  put("/profile/:profileId/topic/:topicId/keyword") {
    logger.info(s"---->   update keyword for this topicID ${params("topicId")}     ")
    val mykeywords = parsedBody.extract[List[Keyword]]
    logger.info(s"---->  update keyword ${mykeywords.size}    ")
    mykeywords.foreach(KeywordDao.updateKeyword)
  }

  delete("/profile/:profileId/topic/:topicId/keyword") {
    logger.info(s"---->   delete list of keyword(s) for this topicID ${params("topicId")}     ")
    val keywordIds = parsedBody.extract[List[Int]]
    logger.info(s"---->   delete list of keyword(s) ${keywordIds.size}     ")
    if (keywordIds.size > 0)
      KeywordDao.deleteKeyword(keywordIds)
  }


  //get supported hospitality sites
  get("/profile/:profileId/datasources/hospitality/all") {
    val validUrl = SocialAccountsHotelDao.getHospitalitySites
    logger.info(s"---->   get supported hospitality sites ")
    validUrl
  }

  // Social accounts

  get("/profile/:profileId/socialchannels/all") {

    val tw = SocialAccountsTwitterDao.getAllAccounts(executor, params("profileId").toInt)
    val fb = SocialAccountsFacebookDao.getAllAccounts(executor, params("profileId").toInt)
    val yt = SocialAccountsYoutubeDao.getAllAccounts(executor, params("profileId").toInt)
    val ga = SocialAccountsGAnalyticsDao.getAllAccounts(executor, params("profileId").toInt)
    val ho = SocialAccountsHotelDao.getAllAccounts(executor, params("profileId").toInt)
    new AsyncResult {
      val is =
        for {
          d1 <- tw
          d2 <- fb
          d3 <- yt
          d4 <- ga
          d5 <- ho
        } yield f1(List(d1, d2, d3, d4, d5))
    }
  }

  def f1(socialAccounts: List[SocialAccounts]) = {
    DataResponseAccounts(200, "Coulio Bro!!!", socialAccounts)
  }


  get("/profile/:profileId/socialchannel/:datasource/:queryId") {
    logger.info(s"---->   return all the social channels for this datasource ${params("datasource")} ")
    params("datasource") match {
      case "twitter" => SocialAccountsTwitterDao.findById(params("profileId").toInt, params("queryId").toInt)
      case "facebook" => SocialAccountsFacebookDao.findById(params("profileId").toInt, params("queryId").toInt)
      case "youtube" => SocialAccountsYoutubeDao.findById(params("profileId").toInt, params("queryId").toInt)
      case "ganalytics" => SocialAccountsGAnalyticsDao.findById(params("profileId").toInt, params("queryId").toInt)
      case "hotel" => SocialAccountsHotelDao.findById(params("profileId").toInt, params("queryId").toInt)
    }

  }


  get("/profile/:profileId/socialchannel/:datasource/all") {
    logger.info(s"---->   return all the social channels for this datasource ${params("datasource")} ")
    params("datasource") match {
      case "twitter" => SocialAccountsTwitterDao.getAllAccounts(executor, params("profileId").toInt)
      case "facebook" => SocialAccountsFacebookDao.getAllAccounts(executor, params("profileId").toInt)
      case "youtube" => SocialAccountsYoutubeDao.getAllAccounts(executor, params("profileId").toInt)
      case "ganalytics" => SocialAccountsGAnalyticsDao.getAllAccounts(executor, params("profileId").toInt)
      case "hotel" => SocialAccountsHotelDao.getAllAccounts(executor, params("profileId").toInt)
    }
  }

  post("/profile/:profileId/socialchannel/:datasource/account") {
    logger.info(s"---->   adds social account for this datasource ${params("datasource")} ")
    params("datasource") match {
      case "twitter" => {
        val account = parsedBody.extract[List[SocialCredentialsTw]]
        logger.info(s"---->   add a new account ${account.size}    ")
        //account.foreach(SocialAccountsTwitterDao.addAccount(params("profileId").toInt, _))
      }
      case "facebook" => {
        logger.info(s"---->   add a new facebook  account ")
        val account = parsedBody.extract[List[SocialCredentialsFb]]
        logger.info(s"---->   add a new account ${account.size}    ")
        account.foreach(SocialAccountsFacebookDao.addAccount(params("profileId").toInt, _))
      }
      case "youtube" => {
        val account = parsedBody.extract[List[SocialCredentialsYt]]
        logger.info(s"---->   add a new account ${account.size}    ")
        account.foreach(SocialAccountsYoutubeDao.addAccount(params("profileId").toInt, _))
      }
      case "ganalytics" => {
        val account = parsedBody.extract[List[SocialCredentialsGa]]
        logger.info(s"---->   add a new account ${account.size}    ")
        account.foreach(SocialAccountsGAnalyticsDao.addAccount(params("profileId").toInt, _))
      }
      case "hotel" => {
        val account = parsedBody.extract[List[SocialCredentialsHotel]]
        logger.info(s"---->   add a new account ${account.size} ")
        account.foreach(SocialAccountsHotelDao.addAccount(params("profileId").toInt, _))
      }
    }

  }

  delete("/profile/:profileId/socialchannel/:datasource/:queryId") {
    logger.info(s"---->   return all the social channels for this datasource ${params("datasource")} ")
    params("datasource") match {
      case "hotel" => SocialAccountsHotelDao.deleteHotel(params("profileId").toInt, params("queryId").toInt)
      case "twitter" => SocialAccountsQueriesDao.deleteSocialCredentials(params("profileId").toInt, params("queryId").toInt)
      case "facebook" => SocialAccountsQueriesDao.deleteSocialCredentials(params("profileId").toInt, params("queryId").toInt)
      case "youtube" => SocialAccountsQueriesDao.deleteSocialCredentials(params("profileId").toInt, params("queryId").toInt)
      case "ganalytics" => SocialAccountsQueriesDao.deleteSocialCredentials(params("profileId").toInt, params("queryId").toInt)
    }
  }

  //first check and then add hotel url
  post("/profile/:profileId/socialchannel/hotel/url") {
    val hotel = parsedBody.extract[SocialCredentialsHotel]
    val validUrl = SocialAccountsHotelDao.checkHotelurl(hotel.hotelUrl)
    if (validUrl) {
      // save hotel in db
      SocialAccountsHotelDao.addAccount(params("profileId").toInt, hotel)
    }
    logger.info(s"---->   check hotel url  ${hotel.hotelUrl} ")
  }


}

case class ConfigurationServlet(executor: ExecutionContext) extends GnostixAPIStack with ConfigApiRoutes
