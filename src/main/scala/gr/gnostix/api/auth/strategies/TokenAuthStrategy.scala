package gr.gnostix.api.auth.strategies

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import gr.gnostix.api.models.pgDao.{User, UserDao}
import org.scalatra.ScalatraBase
import org.scalatra.auth.ScentryStrategy
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

/**
  * Created by rebel on 22/6/16.
  */
class TokenAuthStrategy(protected val app: ScalatraBase)
                       (implicit request: HttpServletRequest, response: HttpServletResponse)
  extends ScentryStrategy[User] {

  val logger = LoggerFactory.getLogger(getClass)

  override def name: String = "TokenAuth"

  object HeaderTokenHelper {

    val ApiHeader = "X-API-Key";
    //    val AppHeader = "X-API-Application";

    lazy val header = Option(request.getHeader(ApiHeader));
    //    lazy val application = Option(request.getHeader(AppHeader));
    // we pass to our proxy nginx the real ip of the user who consumes our API
    // if we work locally we don't have the X-Real-IP header so fo rtesting we get the servername
    val servername = request.getHeaderNames.asScala.contains("X-Real-IP") match {
      case true => request.getHeader("X-Real-IP")
      case false => request.getServerName()
    }

  }

  /* check if the request is valid for token authentication */
  override def isValid(implicit request: HttpServletRequest) = {
    import HeaderTokenHelper._
    logger.info("---------->  TokenStrategy: determining isValid: " + (header != None) )
    header != None // && application != None
  }


  def authenticate()
                  (implicit request: HttpServletRequest, response: HttpServletResponse): Option[User] = {

    import HeaderTokenHelper._

    header match {
      case Some(x) => isValidHostToken(servername, x)
      case _ => None
    }

  }

  /**
    * Check whether the host is valid. This is done by checking the host against
    * a database with keys.
    */
  def isValidHostToken(hostName: String, apiKey: String): Option[User] = {
    UserDao.findByTokenKey(hostName, apiKey);
  }

}
