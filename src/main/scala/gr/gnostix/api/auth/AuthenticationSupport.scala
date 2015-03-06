package gr.gnostix.api.auth


import gr.gnostix.api.auth.strategies.{TheBasicAuthStrategy, UserPasswordStrategy}
import gr.gnostix.api.models.pgDao.{User, UserDao}
import org.scalatra.ScalatraBase
import org.scalatra.auth.{ScentryConfig, ScentrySupport}

//import com.constructiveproof.hackertracker.auth.strategies.RememberMeStrategy

import org.slf4j.LoggerFactory

trait AuthenticationSupport extends ScalatraBase with ScentrySupport[User] {
  self: ScalatraBase =>

  val realm = "Gnostix Basic Auth"
  val logger = LoggerFactory.getLogger(getClass)


  protected val scentryConfig = (new ScentryConfig {}).asInstanceOf[ScentryConfiguration]


  protected def fromSession = {
    case userId: String => {
      logger.info("----> get from SessionStore")
      //User(name,98)
      UserDao.findById(userId.toInt)
    }
  }

  protected def toSession = {
    case user: User => {
      logger.info("-----> store to SessionStore")
      user.userId.toString
    }
  }

  protected def requireLogin() = {
    if (!isAuthenticated) {
      logger.info("------------------> trait:requiredLogin1: not authenticated")
      //halt(401)
      //redirect("/login")
      scentry.authenticate()
      if (!isAuthenticated) {
        logger.info("------------------> trait:requiredLogin2: not authenticated")
        halt(401)
      }
    }
  }

  override protected def configureScentry = {
    scentry.unauthenticated {
      scentry.strategies("UserPassword").unauthenticated()
    }
  }

  override protected def registerAuthStrategies = {
    scentry.register("TheBasicAuth", app => new TheBasicAuthStrategy(app, realm))
    scentry.register("UserPassword", app => new UserPasswordStrategy(app))
    //scentry.register("RememberMe", app => new RememberMeStrategy(app))
  }

}