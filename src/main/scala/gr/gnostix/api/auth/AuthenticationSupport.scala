package gr.gnostix.api.auth


import org.scalatra.ScalatraBase
import org.scalatra.auth.{ScentryConfig, ScentrySupport}
import gr.gnostix.api.models.User
import gr.gnostix.api.auth.strategies.UserPasswordStrategy
//import com.constructiveproof.hackertracker.auth.strategies.RememberMeStrategy
import org.slf4j.LoggerFactory

trait AuthenticationSupport extends ScalatraBase with ScentrySupport[User] {
  self: ScalatraBase =>

  val logger = LoggerFactory.getLogger(getClass)


  protected val scentryConfig = (new ScentryConfig {}).asInstanceOf[ScentryConfiguration]

  protected def fromSession = { case name: String => User(name,87) }
  protected def toSession   = { case usr: User => usr.name }

  protected def requireLogin() = {
    if(!isAuthenticated) {
      //redirect("/sessions/new")
      logger.info("-------------> trait:requiredLogin: was redirected")
      halt(401, "Unauth")
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