import _root_.akka.actor.{ActorSystem, Props}
import gr.gnostix.api.servlets._
import gr.gnostix.api.tmp.{GnxActor, DatafindingsDataServlet}
import org.scalatra._
import javax.servlet.ServletContext
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

//import scala.slick.driver.JdbcDriver.backend.Database
import gr.gnostix.api.db.plainsql.DatabaseAccess

class ScalatraBootstrap extends LifeCycle {

  val logger = LoggerFactory.getLogger(getClass)

  val system = ActorSystem()
  val myActor = system.actorOf(Props[GnxActor])
  protected implicit def executor: ExecutionContext = system.dispatcher

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
    //async
    context.mount(new DatafindingsSentimentLineServlet(executor), "/api/user/datafindings/sentiment/*")
    //context.mount(new TestAsyncServlet(system, myActor), "/api/actors/*")
    //context.mount(new FutureControllerServlet(system), "/api/futures/*")

    // social channels routes
    context.mount(new DatafindingsSentimentLineServlet(executor), "/api/user/socialchannels/line/*")


    // count of data from each datasource
    context.mount(new DatafindingsDataCountServlet(executor), "/api/user/datafindings/counts/*")
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
    system.shutdown()
  }
}

