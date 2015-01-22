package gr.gnostix.api.auth


import gr.gnostix.api.auth.strategies.UserPasswordStrategy
import gr.gnostix.api.models.oraDao.{User, UserDao}
import org.scalatra.ScalatraBase
import org.scalatra.auth.{ScentryConfig, ScentrySupport}
//import com.constructiveproof.hackertracker.auth.strategies.RememberMeStrategy
import org.slf4j.LoggerFactory

trait AuthenticationSupport extends ScalatraBase with ScentrySupport[User]{
  self: ScalatraBase =>

  val logger = LoggerFactory.getLogger(getClass)


  protected val scentryConfig = (new ScentryConfig {}).asInstanceOf[ScentryConfiguration]


  protected def fromSession = { case userId: String => {
                                                        logger.info("----> get from SessionStore")
                                                        //User(name,98)
                                                        UserDao.findById(userId.toInt)
                                                     }
  }
  protected def toSession   = { case user: User => {
                                                      logger.info("-----> store to SessionStore")
                                                      user.userId.toString
  } }

  protected def requireLogin() =  {
    if(!isAuthenticated) {
      //logger.info("------------------> trait:requiredLogin: was redirected")
      halt(401)
      //redirect("/login")
    }
  }


  override protected def configureScentry = {
    scentry.unauthenticated {
      scentry.strategies("UserPassword").unauthenticated()
    }
  }

  override protected def registerAuthStrategies = {
    scentry.register("UserPassword", app => new UserPasswordStrategy(app))
    //scentry.register("RememberMe", app => new RememberMeStrategy(app))
  }

}