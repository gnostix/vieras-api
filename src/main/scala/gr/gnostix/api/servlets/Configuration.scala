package gr.gnostix.api.servlets

import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.models.javaModels.GoogleAnalyticsProfilesJava
import gr.gnostix.api.models.pgDao.SocialAccountsHotelDao.SocialAccountsQueriesDao
import gr.gnostix.api.models.pgDao._
import gr.gnostix.api.models.plainModels._
import gr.gnostix.api.models.publicSearch.{Keyword, KeywordDao, Topic, TopicDao}
import gr.gnostix.api.utilities.{HelperFunctions, GoogleAnalyticsAuth, FbExtendedToken, TwOauth}
import org.json4s.{DefaultFormats, Formats}
import org.scalatra._
import org.scalatra.json._
import twitter4j.auth.AccessToken
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Future, ExecutionContext}


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
    requireLogin()
  }


  //mount point /api/user/account/*


  // profiles data
  get("/profiles/usage") {
    logger.info("---->   return all profiles/usage   ")
    try {
      val usage = ProfilesUsage.findByUserlevel(user.userLevel) //user level....!!!

      ApiMessages.generalSuccess("usage", usage)
    } catch {
      case e: Exception => {
        logger.info("----> Something went wrong" + e.printStackTrace())
        ApiMessages.generalError
      }
    }
  }



  get("/profile/:profileId") {
    logger.info(s"---->   return profile with id ${params("profileId")}     ")
    val profileId = params("profileId").toInt

    val profiledata = ProfileDao.findById(user.userId, profileId)
    ApiData.cleanDataResponse(profiledata)
  }


  get("/profiles/all") {
    logger.info("---->   return all profiles with id and name     " + user.userId)
    try {
      val profiles = ProfileDao.getAllProfiles(user.userId)

      //val companies =  profiles.map{ x => CompanyDao.findAllCompanies(user.userId, x.data.asInstanceOf[Profile].profileId)}

     HelperFunctions.f3(profiles)

      //ApiData.cleanDataResponse(profiles)
    } catch {
      case e: Exception => "Something went wrong" + e.printStackTrace()
        ApiMessages.generalError
    }
  }

  // create a new profile
  post("/profile/:name") {
    logger.info(s"---->   return profile with name ${params("name")}     ")
    val profileId = ProfileDao.createProfile(user.userId, params("name"))

    val response = profileId match {
      case Some(x) => ApiMessages.generalSuccess("profileId", profileId)
      case None => ApiMessages.generalError
    }

    response
  }

  // update the profile name
  put("/profile/:id/:name") {

    val profileId = params("id").toInt
    val profileName = params("name")
    val result = ProfileDao.updateProfileName(user.userId, profileId, profileName)

    val response = result match {
      case Some(x) => Map("status" -> 200, "message" -> "All good", "payload" -> "")
      case None => ApiMessages.generalError
    }

    response
  }

  put("/profile/:id/account") {
    try {
      val profileId = params("id").toInt
      val account = parsedBody.extract[UserAccount]

      UserDao.updateUserAccount(account, user.userId) match {
        case Some(x) => ApiMessages.generalSuccessWithMessage("Account updated")
        case None => ApiMessages.generalError
      }

    } catch {
      case e: Exception => {
        e.printStackTrace()
        ApiMessages.generalErrorOnData
      }
    }


  }

  delete("/profiles/:id") {
    // not implemented
  }



// COMPANIES

  get("/profile/:profileId/company/:companyId") {
    logger.info(s"---->   return company with id ${params("id")}     ")
    val profileId = params("profileId").toInt
    val companyId = params("companyId").toInt
    val companyData = CompanyDao.findById(user.userId, profileId, companyId)
    ApiData.cleanDataResponse(companyData)
  }

  get("/profile/:profileId/company/mycompany") {
    logger.info(s"---->   return company with id ${params("id")}     ")
    val profileId = params("profileId").toInt

    val companyData = CompanyDao.getMyCompany(user.userId, profileId)
    ApiData.cleanDataResponse(companyData)
  }

  get("/profiles/:profileId/company/all") {
    logger.info("---->   return all profiles with id and name     " + user.userId)
    try {
      val profileId = params("profileId").toInt
      val company = CompanyDao.findAllCompanies(user.userId,profileId)
      ApiData.cleanDataResponse(company)
    } catch {
      case e: Exception => "Something went wrong" + e.printStackTrace()
        ApiMessages.generalError
    }
  }

  // create a new Company
  post("/profile/:profileId/company") {
    logger.info(s"---->   return company with name ${params("name")}     ")
    val profileId = params("profileId").toInt
    val company = parsedBody.extract[CompanyGroup]
    val companyId = CompanyDao.createCompany(profileId, company)

    val response = companyId match {
      case Some(x) => ApiMessages.generalSuccess("companyId", companyId)
      case None => ApiMessages.generalError
    }

    response
  }

  // update the profile name
  put("/profile/:profileId/company/:companyId/:name") {

    val profileId = params("profileId").toInt
    val companyName = params("name")
    val companyId = params("companyId").toInt
    val result = CompanyDao.updateName(user.userId, profileId, companyId, companyName)

    val response = result match {
      case Some(x) => ApiMessages.generalSuccessNoData
      case None => ApiMessages.generalError
    }

    response
  }

  delete("/profiles/:profileId/company/:companyId") {
    val profileId = params("profileId").toInt
    val companyId = params("companyId").toInt
    val result = CompanyDao.deleteCompany(user.userId, profileId, companyId)

    val response = result match {
      case Some(x) => ApiMessages.generalSuccessNoData
      case None => ApiMessages.generalError
    }

    response
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

  get("/profile/:profileId/ga/sites") {

    val status: Int = session.getAttribute("status_ga").asInstanceOf[Int]

    status match {
      case 200 => {
        val sitesToMonitor = session.getAttribute("sites_for_monitor").asInstanceOf[java.util.List[GoogleAnalyticsProfilesJava]]
        logger.info(s"---->  sites " + sitesToMonitor.toString)

        ApiMessages.generalSuccess("sites", sitesToMonitor.asScala.toList.map(x =>
          GoogleAnalyticsProfiles(x.getAccountId, x.getWebpropertyId, x.getProfileid, x.getProfileName)))
      }
      case _ => ApiMessages.pending
    }


  }

  // facebook auth
  post("/profile/:id/fb/pages") {
    try {
      val fbToken = parsedBody.extract[FacebookToken]
      val profileId = params("id").toInt

      logger.info("---->   GET FB TOKEN !!!!    ")
      val token = FbExtendedToken.getExtendedToken(fbToken.token)
      val pages = FbExtendedToken.getUserPages(token.getAccessToken)
      val data = FacebookPageAuth(token.getAccessToken, token.getExpires, pages.toList)
      DataResponse(200, "All good", data)
    } catch {
      case e: Exception => "Something went wrong" + e.printStackTrace()
        Map("status" -> 400, "message" -> "Something went wrong")
    }
  }


  // Twitter auth
  // 1. Step - Give the user the url for accepting the gnostix app
  get("/profile/:id/tw/auth") {
    logger.info("---->   Twitter AUTH!!!!    ")
    var twAuth: TwOauth = session.getAttribute("twitter_auth").asInstanceOf[TwOauth]
    var urlAuth: String = "";

    if (twAuth == null) {
      twAuth = new TwOauth()
      session.setAttribute("twitter_auth", twAuth)
    }

    urlAuth = twAuth.getRequestToken.getAuthorizationURL
    urlAuth match {
      case null => Map("status" -> 400, "message" -> "Something went wrong")
      case x => Map("status" -> 200, "message" -> "all good", "payload" -> Map("url" -> urlAuth))
    }
  }

  // 2. Step - Get the authorization of Twitter and save the account
  post("/profile/:id/company/:companyId/tw/auth/:pin") {
    logger.info("---->   Twitter PIN !!!!    ")
    var twAuth: TwOauth = session.getAttribute("twitter_auth").asInstanceOf[TwOauth]

    val profileId = params("id").toInt
    val companyId = params("companyId").toInt
    // add twitter account and then return the twitter handle
    val accessToken: AccessToken = twAuth.getUserToken(params("pin"))
    val account = SocialAccountsTwitterDao.addAccount(profileId, companyId, accessToken.getToken(), accessToken.getTokenSecret(),
      accessToken.getScreenName())

    // reset token so user can get more accounts
    session.setAttribute("twitter_auth", null)
    account match {
      case Some(data) => Map("status" -> 200, "message" -> "all good", "payload" -> data)
      case None => Map("status" -> 402, "message" -> "error on adding account")
    }
  }



  //get supported hospitality sites
  get("/profile/:profileId/company/:companyId/socialchannel/hospitality/supported/all") {
    val validUrl = SocialAccountsHotelDao.getHospitalitySites
    logger.info(s"---->   get supported hospitality sites ")
    Map("status" -> 200, "message" -> "all good", "payload" -> validUrl)

  }

  // get customer hotel sites
  get("/profile/:profileId/company/:companyId/socialchannel/hospitality/hotel/all") {
    val profileId = params("profileId").toInt
    val companyId = params("companyId").toInt

    val hotelUrls = SocialAccountsHotelDao.getHotelUrls(profileId, companyId)
    logger.info(s"---->   get customer hotel sites ")
    Map("status" -> 200, "message" -> "all good", "payload" -> hotelUrls)
  }

  // ====================== Social accounts ====================================

  get("/profile/:profileId/company/:companyId/socialchannels/all") {

    val profileId = params("profileId").toInt
    val companyId = params("companyId").toInt

    val tw = SocialAccountsTwitterDao.getAllAccounts(executor, profileId, companyId)
    val fb = SocialAccountsFacebookDao.getAllAccounts(executor, profileId, companyId)
    val yt = SocialAccountsYoutubeDao.getAllAccounts(executor, profileId, companyId)
    val ga = SocialAccountsGAnalyticsDao.getAllAccounts(executor, profileId, companyId)
    val ho = SocialAccountsHotelDao.getAllAccounts(executor, profileId, companyId)
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

  def f1(socialAccounts: List[Option[SocialAccounts]]) = {
    logger.info(s"---->   socialAccounts ?? ${socialAccounts.isEmpty} ")
    val sent = ArrayBuffer[SocialAccounts]()
    for (a <- socialAccounts) {
      a match {
        case Some(s: SocialAccounts) => sent.+=(s)
        case None => logger.info(s"-----> None => do nothing..}")
      }
    }
    Map("status" -> 200, "message" -> "all good", "payload" -> sent)
  }


  //i need to refactor these
  get("/profile/:profileId/company/:companyId/socialchannel/:datasource/:credId") {
    logger.info(s"---->   return all the social channels for this datasource ${params("datasource")} ")

    val profileId = params("profileId").toInt
    val companyId = params("companyId").toInt
    val credId = params("credId").toInt

    params("datasource") match {
      case "twitter" => SocialAccountsTwitterDao.findById(profileId, companyId, credId)
      case "facebook" => SocialAccountsFacebookDao.findById(profileId, companyId, credId)
      case "youtube" => SocialAccountsYoutubeDao.findById(profileId, companyId, credId)
      case "ganalytics" => SocialAccountsGAnalyticsDao.findById(profileId, companyId, credId)
      case "hotel" => SocialAccountsHotelDao.findById(profileId, companyId, credId)
    }

  }


  get("/profile/:profileId/company/:companyId/socialchannel/:datasource/all") {
    logger.info(s"---->   return all the social channels for this datasource ${params("datasource")} ")

    val profileId = params("profileId").toInt
    val companyId = params("companyId").toInt

    val data = params("datasource") match {
      case "twitter" => SocialAccountsTwitterDao.getAllAccounts(executor, profileId, companyId)
      case "facebook" => SocialAccountsFacebookDao.getAllAccounts(executor, profileId, companyId)
      case "youtube" => SocialAccountsYoutubeDao.getAllAccounts(executor, profileId, companyId)
      // the next route needs refactor !!!!!!!!!!!
      case "ganalytics" => SocialAccountsGAnalyticsDao.getAllAccounts(executor, profileId, companyId)
      case "hotel" => SocialAccountsHotelDao.getAllAccounts(executor, profileId, companyId)
    }
    ApiMessages.generalSuccessOneParam(data)
  }

  // add social and hospitality accounts
  post("/profile/:profileId/company/:companyId/socialchannel/:datasource/account") {
    logger.info(s"---->   adds social account for this datasource ${params("datasource")} ")

    val profileId = params("profileId").toInt
    val companyId = params("companyId").toInt

    params("datasource") match {
      case "twitter" => {
        // we add the twitter account on /profile/:id/tw/auth/:pin !!!!!!!!!!!!!!!!!!!!!!!!!
        //val account = parsedBody.extract[List[SocialCredentialsTw]]
        logger.info(s"---->   add a new account. the account is added from the Auth route   !!!!! ")
        //------------------account.foreach(SocialAccountsTwitterDao.addAccount(params("profileId").toInt, _))
      }
      case "facebook" => {
        logger.info(s"---->   add a new facebook  account ")
        val account = parsedBody.extract[SocialCredentialsFb]
        logger.info(s"---->   add a new account ${account}    ")
        val data = SocialAccountsFacebookDao.addAccount(profileId, companyId, account)
        data match {
          case Some(x) => Map("status" -> 200, "message" -> "all good", "payload" -> data)
          case None => Map("status" -> 400, "message" -> "Error")
        }
      }
      case "youtube" => {
        val account = parsedBody.extract[SocialCredentialsYt]
        logger.info(s"---->   add a new account ${account}    ")
        val data = SocialAccountsYoutubeDao.addAccount(profileId, companyId, account)
        data match {
          case Some(x) => Map("status" -> 200, "message" -> "all good", "payload" -> data)
          case None => Map("status" -> 400, "message" -> "Error")
        }
      }
      case "ganalytics" => {

        val account = parsedBody.extract[GoogleAnalyticsProfiles]
        val token = session.getAttribute("ga_token").toString
        val refreshToken = session.getAttribute("ga_refresh_token").toString

        if (token != null && refreshToken != null) {

          logger.info(s"---->   add a new account ${account}    ")
          val data = SocialAccountsGAnalyticsDao.addAccount(profileId, companyId, token, refreshToken, account)

          // clean session from ga tokens
          session.removeAttribute("ga_token")
          session.removeAttribute("ga_refresh_token")
          data match {
            case Some(x) => Map("status" -> 200, "message" -> "all good", "payload" -> data)
            case None => Map("status" -> 400, "message" -> "Error")
          }
        } else {
          Map("status" -> 400, "message" -> "Error")
        }
      }
      case "hotel" => {
        val hotel = parsedBody.extract[SocialCredentialsHotel]
        val validUrl = SocialAccountsHotelDao.checkHotelUrl(hotel.hotelUrl)
        logger.info(s"---->   validUrl $validUrl ")
        if (validUrl._2) {
          // save hotel in db
          val credId = SocialAccountsHotelDao.addAccount(profileId, companyId, hotel, validUrl._3)

          credId match {
            case Some(x) => {
              logger.info(s"---->   hotelId $x ")
              Map("status" -> 200, "message" -> "all good", "payload" -> Map("credId" -> x))
            }
            case None => Map("status" -> 402, "message" -> "Something went wrong")
          }

        } else {
          Map("status" -> 402, "message" -> validUrl._1)
        }

      }
    }

  }

  delete("/profile/:profileId/company/:companyId/socialchannel/:datasource/:credId") {
    logger.info(s"---->   delete social channel for this datasource ${params("datasource")} ")

    val profileId = params("profileId").toInt
    val companyId = params("companyId").toInt

    var status: Int = 100
    val credId = params("credId").toInt
    params("datasource") match {
      case "hotel" => status = SocialAccountsQueriesDao.deleteSocialAccount(profileId, companyId, credId, params("datasource")).get
      case "twitter" => status = SocialAccountsQueriesDao.deleteSocialAccount(profileId, companyId, credId, params("datasource")).get
      case "facebook" => status = SocialAccountsQueriesDao.deleteSocialAccount(profileId, companyId, credId, params("datasource")).get
      case "youtube" => status = SocialAccountsQueriesDao.deleteSocialAccount(profileId, companyId, credId, params("datasource")).get
      case "ganalytics" => status = SocialAccountsQueriesDao.deleteSocialAccount(profileId, companyId, credId, params("datasource")).get
    }
    status match {
      case 200 => Map("status" -> 200, "message" -> "all good")
      case 400 => Map("status" -> 400, "message" -> "something went wrong")
      case _ => Map("status" -> 500, "message" -> "Server error! Something went very wrong")
    }

  }


}

case class ConfigurationServlet(executor: ExecutionContext) extends GnostixAPIStack with ConfigApiRoutes
