package gr.gnostix.api.servlets

import gr.gnostix.api.GnostixAPIStack
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.models.javaModels.GoogleAnalyticsTokens
import gr.gnostix.api.models.pgDao.{UserDao, UserRegistration}
import gr.gnostix.api.models.plainModels.{ApiMessages, AllDataResponse}
import gr.gnostix.api.utilities.{GoogleAnalyticsAuth, EmailUtils}
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
      AllDataResponse(200, "all good", List(user))
    } else {
      logger.info("-----------------------> /login: NOT successful")
      halt(401)
    }
  }

  post("/logout") {
    scentry.logout()
  }

  get("/getUserInfo") {
    requireLogin()
    user.password = ""
    user
    AllDataResponse(200, "all good", List(user))
  }

  post("/register") {
    logger.info("-----------------------> /register")
    try {
      val regUser = parsedBody.extract[UserRegistration]
      val existingUser = UserDao.findByUsername(regUser.username)

      existingUser match {
        case Some(x) => ApiMessages.generalErrorWithMessage("user already exists!")
        case None => {
          if (regUser.email.matches(emailRegex.toString())) {
            val status = UserDao.createUser(regUser)

            status match {
              case Some(x) => ApiMessages.generalSuccessWithMessage("Account created ...")
              case None => ApiMessages.generalErrorWithMessage("error on account creation ")
            }


          }
          else {
            ApiMessages.generalErrorWithMessage("invalid email!")
          }
        }
      }
    } catch {
      case e: Exception => ApiMessages.generalErrorWithMessage("error on data ")
    }

  }


  post("/reminder") {
    logger.info("-----------------------> /reminder")
    try {
      val email: String = parsedBody.extract[String]

      val validateEmail = email.matches(emailRegex.toString())

      validateEmail match {
        case true => {
          UserDao.findByUsername(email) match {
            case Some(x) => {

              UserDao.resetPassword(x) match {
                case Some(a) => ApiMessages.generalSuccessWithMessage("Reminding the user password. Email send...")
                case None => ApiMessages.generalErrorWithMessage("error on reseting user's password ")
              }

            }
            case None => ApiMessages.generalErrorWithMessage("user doesn't exists!")
          }

        }
        case false => ApiMessages.generalErrorWithMessage("invalid email!")
      }


    } catch {
      case e: Exception => ApiMessages.generalErrorWithMessage("error on data ")
    }

  }


  post("/checksignupemail") {
    try {
      val email: String = parsedBody.extract[String]
      val validateEmail = email.matches(emailRegex.toString())

      validateEmail match {
        case true => {
          UserDao.findByUsername(email) match {
            case Some(x) => ApiMessages.generalSuccessWithMessage("user exists")
            case None => ApiMessages.generalErrorWithMessage("user doesn't exists!")
          }
        }
        case false => ApiMessages.generalErrorWithMessage("invalid email!")

      }
    } catch {
      case e: Exception => ApiMessages.generalErrorWithMessage("error on data ")
    }
  }

  get("/ga") {
    val code = params("code")
    val state = params("state")
    logger.info(s"---->  google analytics auth code $code  ")
    logger.info(s"---->  google analytics auth state $state  ")
    //val gAuth: GoogleAnalyticsAuth = new GoogleAnalyticsAuth()
    //val tokens: GoogleAnalyticsTokens = gAuth.requestAccessToken(code)

    redirect("/api/ga/withsession;jsessionid=" + state)

  }


  get("/ga/withsession*") {
    requireLogin()
    logger.info("the user session name: " + user.username)
    logger.info("the user session details: " + user.userDetails)
    logger.info("the user session id: " + session.getId)

/*    tokens.getStatus match {
      case 200 => {
        session.setAttribute("ga_token", tokens.getToken)
        session.setAttribute("ga_refresh_token", tokens.getRefreshToken)

        val gAuth: GoogleAnalyticsAuth = new GoogleAnalyticsAuth()
        val sitesToMonitor = gAuth.getUserSitesToMonitor(tokens.getToken, tokens.getRefreshToken)
        session.setAttribute("sites_for_monitor", sitesToMonitor)
        session.setAttribute("status_ga", 200)


        <html>
          <body>
            <h1>Authorization ok! Please close this window and return to Vieras app.</h1>
          </body>
        </html>

      }
      case 400 => {
        <html>
          <body>
            <h1>Error on Google Authentication</h1>
          </body>
        </html>
      }

    }*/
  }

  private val emailRegex = """^(?!\.)("([^"\r\\]|\\["\r\\])*"|([-a-zA-Z0-9!#$%&'*+/=?^_`{|}~]|(?<!\.)\.)*)(?<!\.)@[a-zA-Z0-9][\w\.-]*[a-zA-Z0-9]\.[a-zA-Z][a-zA-Z\.]*[a-zA-Z]$""".r

}

case class MyScalatraServlet() extends GnostixAPIStack with RestApiRoutes

