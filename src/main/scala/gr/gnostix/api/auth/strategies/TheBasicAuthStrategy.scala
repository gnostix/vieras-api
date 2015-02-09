package gr.gnostix.api.auth.strategies


import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import gr.gnostix.api.models.pgDao.{UserDao, User}
import org.scalatra.ScalatraBase
import org.scalatra.auth.strategy.BasicAuthStrategy
import org.slf4j.LoggerFactory


class TheBasicAuthStrategy(protected override val app: ScalatraBase, realm: String)
  extends BasicAuthStrategy[User](app, realm) {

  override def name: String = "Bill"

  val logger = LoggerFactory.getLogger(getClass)

  override protected def getUserId(user: User)(implicit request: HttpServletRequest, response: HttpServletResponse): String = "10"

  override def isValid(implicit request: HttpServletRequest) = {
    logger.info("-----------> TheBasicAuthStrategy: isValid " + app.request.isBasicAuth +" " + app.request.providesAuth)
      app.request.isBasicAuth && app.request.providesAuth
  }

  override protected def validate(userName: String, password: String)(implicit request: HttpServletRequest, response: HttpServletResponse): Option[User] = {
    logger.info("TheBasicAuthStrategy: found the username in DB. userName: " + userName)
    UserDao.findByUsername(userName) match {
      case Some(user) => {
        logger.info("TheBasicAuthStrategy: found the username in DB")
        if (true) {
          Some(user)
        } else {
          logger.info("-----------> TheBasicAuthStrategy: login failed --> user and pass did not match!!");
          None
        }
      }
      case None => {
        logger.info("-----------> TheBasicAuthStrategy: login failed")
        None
      }
    }
  }
}
