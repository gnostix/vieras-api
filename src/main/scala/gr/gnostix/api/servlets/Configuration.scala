package gr.gnostix.api.servlets

import org.scalatra._
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json._
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.models._


trait ConfigApiRoutes extends ScalatraServlet
with JacksonJsonSupport
with AuthenticationSupport
with CorsSupport
with MethodOverride {

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
    logger.info(s"---->   return all the topics for this profileId ${params("id")}     ")
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
    if (topicIds.size > 0 )
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
    if (keywordIds.size > 0 )
      KeywordDao.deleteKeyword(keywordIds)
  }

}

case class ConfigurationServlet() extends GnostixAPIStack with ConfigApiRoutes
