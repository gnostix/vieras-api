import gr.gnostix.api.servlets._
import gr.gnostix.api.tmp.DatafindingsDataServlet
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

    context.mount(new MyScalatraServlet(), "/api/*")

    context.mount(new ConfigurationServlet(), "/api/user/account/*")

    context.mount(new DatafindingsLineServlet(), "/api/user/datafindings/line/*")
    context.mount(new DatafindingsDataServlet(), "/api/user/datafindings/raw/*")
    context.mount(new DatafindingsFirstLevelDataServlet(), "/api/user/datafindings/raw/firstlevel/*")
    context.mount(new DatafindingsSecondLevelDataServlet(), "/api/user/datafindings/raw/secondlevel/*")
    context.mount(new DatafindingsThirdLevelDataServlet(), "/api/user/datafindings/raw/thirdlevel/*")
    context.mount(new DatafindingsSentimentLineServlet(), "/api/user/datafindings/sentiment/*")
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

