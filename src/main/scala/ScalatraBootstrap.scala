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

    context.mount(new ConfigurationServlet(executor), "/api/user/account/*")

    context.mount(new DatafindingsLineServlet(), "/api/user/datafindings/line/*")
    context.mount(new DatafindingsDataServlet(), "/api/user/datafindings/raw/*")
    context.mount(new DatafindingsFirstLevelDataServlet(), "/api/user/datafindings/raw/firstlevel/*")
    context.mount(new DatafindingsSecondLevelDataServlet(), "/api/user/datafindings/raw/secondlevel/*")
    context.mount(new DatafindingsThirdLevelDataServlet(), "/api/user/datafindings/raw/thirdlevel/*")
    //async
    context.mount(new DatafindingsSentimentLineServlet(executor), "/api/user/datafindings/sentiment/*")
    //context.mount(new TestAsyncServlet(system, myActor), "/api/actors/*")
    //context.mount(new FutureControllerServlet(system), "/api/futures/*")

    // count of data from each datasource
    context.mount(new DatafindingsDataCountServlet(executor), "/api/user/datafindings/counts/*")

    // social channels routes
    context.mount(new SocialChannelsFacebookLineServlet(executor), "/api/user/socialchannels/facebook/line/*")
    context.mount(new SocialChannelsTwitterLineServlet(executor), "/api/user/socialchannels/twitter/line/*")
    context.mount(new HospitalityLineCountsServlet(executor), "/api/user/socialchannels/hotel/line/*")
    context.mount(new SocialChannelsYoutubeLineServlet(executor), "/api/user/socialchannels/youtube/line/*")

    // hospitality services
    context.mount(new HospitalityServicesServlet(executor), "/api/user/account/hospitality/services/*")

    // dashboard sum data
    context.mount(new DashboardSumDataServlet(executor), "/api/user/account/dashboard/services/*")

    // GeoLocation services
    context.mount(new GeoServicesServlet(executor), "/api/user/account/geolocation/services/*")

    // dashboard social pages
    context.mount(new FacebookDashboardServlet(executor), "/api/user/socialchannels/dashboard/facebook/*")
    context.mount(new TwitterDashboardServlet(executor), "/api/user/socialchannels/dashboard/twitter/*")
    context.mount(new YoutubeDashboardServlet(executor), "/api/user/socialchannels/dashboard/youtube/*")

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



