package gr.gnostix.api.auth.strategies

import org.scalatra.ScalatraBase
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import org.scalatra.auth.ScentryStrategy
import org.slf4j.LoggerFactory
import gr.gnostix.api.models.{UserDao, User}

class UserPasswordStrategy(protected val app: ScalatraBase)
                          (implicit request: HttpServletRequest, response: HttpServletResponse)
  extends ScentryStrategy[User] {

  //val db: Database
  override def name: String = "UserPassword"

  val logger = LoggerFactory.getLogger(getClass)


  private def username = app.params.getOrElse("username", "")

  private def password = app.params.getOrElse("password", "")

  /** *
    * Determine whether the strategy should be run for the current request.
    */
  override def isValid(implicit request: HttpServletRequest) = {
    logger.info("---------->  UserPasswordStrategy: determining isValid: " + (username != "" && password != "").toString())
    username != "" && password != ""
  }


  /**
   * In real life, this is where we'd consult our data store, asking it whether the user credentials matched
   * any existing user. Here, we'll just check for a known login/password combination and return a user if
   * it's found.
   */
  def authenticate()(implicit request: HttpServletRequest, response: HttpServletResponse): Option[User] = {
    logger.info("UserPasswordStrategy: attempting authentication")


    UserDao.findByUsername(username) match {
      case Some(user) => {
        logger.info("UserPasswordStrategy: found the username in DB")
        if (checkUserPassword(username, password, user.password)) {
          Some(user)
        } else {
          logger.info("-----------> UserPasswordStrategy: login failed --> user and pass did not match!!");
          None
        }
      }
      case None => {
        logger.info("-----------> UserPasswordStrategy: login failed");
        None
      }
    }
  }

  /**
   * What should happen if the user is currently not authenticated?
   */
  override def unauthenticated()(implicit request: HttpServletRequest, response: HttpServletResponse) {
    //app.redirect("/sessions/new")
    logger.info("---------> UserPasswordStrategy: login unauthenticated, was redirected")
    app.redirect("/login")
  }

  def checkUserPassword(username: String, password: String, userDbPassword: String): Boolean = {
    logger.info (s"---------> UserPasswordStrategy checkUserPassword :  $username $password")
    if (md5Hash (username + password) == userDbPassword) true else false
  }

  // create a md5 hash from a string
  def md5Hash(text: String): String = java.security.MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map {
    "%02x".format(_)
  }.foldLeft("") {
    _ + _
  }
}