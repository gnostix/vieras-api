package gr.gnostix.api.auth.strategies

import org.scalatra.ScalatraBase
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import org.scalatra.auth.ScentryStrategy
import org.slf4j.LoggerFactory
import gr.gnostix.api.models.User

class UserPasswordStrategy(protected val app: ScalatraBase)
                          (implicit request: HttpServletRequest, response: HttpServletResponse)
  extends ScentryStrategy[User] {

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

    if (username == "foo" && password == "foo") {
      logger.info("UserPasswordStrategy: login succeeded")
      Some(User("Alex Pappas", 25))
    } else {
      logger.info("-----------> UserPasswordStrategy: login failed")
      println("--------> auth failed")
      None
    }
  }

  /**
   * What should happen if the user is currently not authenticated?
   */
  override def unauthenticated()(implicit request: HttpServletRequest, response: HttpServletResponse) {
    //app.redirect("/sessions/new")
    logger.info("---------> UserPasswordStrategy: login unauthenticated, was redirected")

  }
}