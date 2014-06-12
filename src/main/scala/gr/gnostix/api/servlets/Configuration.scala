package gr.gnostix.api.servlets

import org.scalatra._
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json._
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.models.{ProfilesUsage, ProfileDao, TopicDao, KeywordDao}


trait ConfigApiRoutes extends ScalatraServlet
with JacksonJsonSupport
with AuthenticationSupport
with CorsSupport {

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
    ProfileDao.findById(params("id").toInt)
  }


  get("/profile/:profileId/topics/all") {
    logger.info(s"---->   return all the topics for this profileId ${params("profileId")}     ")
    TopicDao.getAllTopics(params("profileId").toInt)
  }


  get("/profile/:profileId/topic/:id") {
    logger.info(s"---->   return all the topics for this profileId ${params("id")}     ")
    TopicDao.findById(params("id").toInt)
  }


  get("/profile/:profileId/topic/:topicId/keywords/all") {
    logger.info(s"---->   return all the keywords for this topicID ${params("topicId")}     ")
    KeywordDao.getAllKeywords(params("topicId").toInt)
  }

}

case class ConfigurationServlet() extends GnostixAPIStack with ConfigApiRoutes
