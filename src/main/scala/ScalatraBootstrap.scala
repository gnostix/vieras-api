import com.mchange.v2.c3p0.ComboPooledDataSource
import gr.gnostix.api._
import gr.gnostix.api.models.DB
import javax.sql.DataSource
import org.scalatra._
import javax.servlet.ServletContext
import org.slf4j.LoggerFactory
//import scala.slick.driver.JdbcDriver.backend.Database
import scala.slick.jdbc.JdbcBackend.Database

class ScalatraBootstrap extends LifeCycle {

  val logger = LoggerFactory.getLogger(getClass)

  val cpds = new ComboPooledDataSource
  logger.info("-->  Created c3p0 connection pool")

  //
  override def init(context: ServletContext) {
    val db = Database.forDataSource(cpds)  // create a Database which uses the DataSource
    context.mount(new MyScalatraServlet(db), "/*")
  }

  //
  private def closeDbConnection() {
    logger.info("-->  Closing c3po connection pool")
    cpds.close
  }

  //
  override def destroy(context: ServletContext) {
    super.destroy(context)
    closeDbConnection
  }
}

