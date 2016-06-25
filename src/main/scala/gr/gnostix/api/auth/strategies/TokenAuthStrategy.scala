package gr.gnostix.api.auth.strategies

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import gr.gnostix.api.models.pgDao.{User, UserDao}
import org.scalatra.ScalatraBase
import org.scalatra.auth.ScentryStrategy

/**
  * Created by rebel on 22/6/16.
  */
class TokenAuthStrategy(protected val app: ScalatraBase)
                       (implicit request: HttpServletRequest, response: HttpServletResponse)
  extends ScentryStrategy[User] {


  object HeaderTokenHelper {

    val ApiHeader = "X-API-Key";
    //    val AppHeader = "X-API-Application";

    lazy val header = Option(request.getHeader(ApiHeader));
    //    lazy val application = Option(request.getHeader(AppHeader));
    lazy val servername = request.getServerName();

  }

  /* check if the request is valid for token authentication */
  override def isValid(implicit request: HttpServletRequest) = {
    import HeaderTokenHelper._
    header != None // && application != None
  }


  def authenticate()
                  (implicit request: HttpServletRequest, response: HttpServletResponse): Option[User] = {

    import HeaderTokenHelper._

    header match {
      case Some(x) => isValidHostToken(servername, x)
      case _ => None
    }

    None
  }

  /**
    * Check whether the host is valid. This is done by checking the host against
    * a database with keys.
    */
  def isValidHostToken(hostName: String, apiKey: String): Option[User] = {
    UserDao.findByTokenKey(hostName, apiKey);
  }

}
