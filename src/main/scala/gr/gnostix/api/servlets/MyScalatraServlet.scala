package gr.gnostix.api.servlets

import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.models.plainModels.AllDataResponse
import org.json4s.{DefaultFormats, Formats}
import org.scalatra._
import org.scalatra.json._


trait RestApiRoutes extends ScalatraServlet
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
  }

  post("/login") {
    scentry.authenticate()
    if (isAuthenticated) {
      logger.info("--------------> /login: successful Id: " + user.userId)
      user.password = ""
      user
      AllDataResponse(200,"all good",List(user))
    } else {
      logger.info("-----------------------> /login: NOT successful")
      halt(401)
    }
  }

  post("/logout") {
    scentry.logout()
    //redirect("/login")
  }

  get("/getUserInfo") {
    requireLogin()
    user.password = ""
    user
    AllDataResponse(200,"all good",List(user))
  }

}

case class MyScalatraServlet() extends GnostixAPIStack with RestApiRoutes

