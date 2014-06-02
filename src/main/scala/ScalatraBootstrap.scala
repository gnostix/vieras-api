import com.mchange.v2.c3p0.ComboPooledDataSource
import gr.gnostix.api._
import javax.sql.DataSource
import org.scalatra._
import javax.servlet.ServletContext
import org.slf4j.LoggerFactory
//import scala.slick.driver.JdbcDriver.backend.Database
import scala.slick.jdbc.JdbcBackend.Database
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

