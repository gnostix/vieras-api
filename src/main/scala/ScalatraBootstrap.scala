import gr.gnostix.api.servlets.{ConfigurationServlet, MyScalatraServlet}
import org.scalatra._
import javax.servlet.ServletContext
import org.slf4j.LoggerFactory
//import scala.slick.driver.JdbcDriver.backend.Database
import gr.gnostix.api.db.plainsql.DatabaseAccess

class ScalatraBootstrap extends LifeCycle {

  val logger = LoggerFactory.getLogger(getClass)

  DatabaseAccess.createDatasource
  //val cpds = new ComboPooledDataSource
  logger.info("-->  Created c3p0 connection pool")

  //
  override def init(context: ServletContext) {
    val db = DatabaseAccess.database
    logger.info("-->  create a Database")
    //val db = Database.forDataSource(cpds)  // create a Database which uses the DataSource

    context.mount(new MyScalatraServlet(), "/*")

    context.mount(new ConfigurationServlet(), "/api/user/*")
    //context.initParameters("org.scalatra.cors.allowedMethods") = "GET, PUT, DELETE, HEAD, OPTIONS, POST"
    //context.initParameters("org.scalatra.cors.allowedOrigins") = "http://192.168.2.23:8080"

  }

  //
  private def closeDbConnection() {
    logger.info("-->  Closing c3po connection pool")
    //cpds.close
    DatabaseAccess.closeDBPool
  }

  //
  override def destroy(context: ServletContext) {
    super.destroy(context)
    closeDbConnection
  }
}

