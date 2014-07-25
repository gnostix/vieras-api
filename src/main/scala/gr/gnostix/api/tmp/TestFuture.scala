package gr.gnostix.api.tmp

import _root_.akka.actor.ActorSystem
import gr.gnostix.api.auth.AuthenticationSupport
import gr.gnostix.api.models._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s.{DefaultFormats, Formats}
import org.scalatra._
import org.scalatra.json.JacksonJsonSupport

import scala.concurrent.{ExecutionContext, Future, Promise}

class FutureControllerServlet(system: ActorSystem) extends ScalatraServlet
with JacksonJsonSupport
with AuthenticationSupport
with CorsSupport
with FutureSupport {

  options("/*") {
    response.setHeader("Access-Control-Allow-Headers", request.getHeader("Access-Control-Request-Headers"))
  }

  protected implicit val jsonFormats: Formats = DefaultFormats

  protected implicit def executor: ExecutionContext = system.dispatcher

  before() {
    contentType = formats("json")
  }

  get("/koko") {
    logger.info("---------------> /koko")

    val aa = SentimentDataFuture.getFuture
    val bb = SentimentDataFuture.getFuture
    new AsyncResult {
      val is =
        for {
          a <- aa
          b <- bb
        } yield List(a, b)
    }
  }

  get("/koko1") {
    logger.info("---------------> /koko1")

    val aa = SentimentDataFuture.getSent
    val bb = SentimentDataFuture.getSent
    new AsyncResult {
      val is =
        for {
          a <- aa
          b <- bb
        } yield List(a ++ b)
    }
  }

  get("/profile/:profileId/:fromDate/:toDate/all") {

    val fromDate: DateTime = DateTime.parse(params("fromDate"),
      DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
    logger.info(s"---->   parsed date ---> ${fromDate}    ")

    val toDate: DateTime = DateTime.parse(params("toDate"),
      DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:ss"))
    logger.info(s"---->   parsed date ---> ${toDate}    ")

    val profileId = params("profileId").toInt

    val dt1 = FutureSentimentDao.getDataDefault(executor, fromDate, toDate, profileId, "twitter")
    val dt2 = FutureSentimentDao.getDataDefault(executor, fromDate, toDate, profileId, "facebook")
    val dt3 = FutureSentimentDao.getDataDefault(executor, fromDate, toDate, profileId, "youtube")
    val dt4 = FutureSentimentDao.getDataDefault(executor, fromDate, toDate, profileId, "gplus")
    val dt5 = FutureSentimentDao.getDataDefault(executor, fromDate, toDate, profileId, "web")
    val dt6 = FutureSentimentDao.getDataDefault(executor, fromDate, toDate, profileId, "linkedin")
    val dt7 = FutureSentimentDao.getDataDefault(executor, fromDate, toDate, profileId, "news")
    val dt8 = FutureSentimentDao.getDataDefault(executor, fromDate, toDate, profileId, "blog")
    val dt9 = FutureSentimentDao.getDataDefault(executor, fromDate, toDate, profileId, "personal")
    new AsyncResult {
      val is =
        for {
          a1 <- dt1
          a2 <- dt2
          a3 <- dt3
          a4 <- dt4
          a5 <- dt5
          a6 <- dt6
          a7 <- dt7
          a8 <- dt8
          a9 <- dt9
        } yield f1(List(a1.get, a2.get, a3.get, a4.get, a5.get, a6.get, a7.get, a8.get, a9.get))
    }
  }

  def f3(a: SocialData, b: SocialData) = {

    val k = (a.data ++ b.data).groupBy(_.asInstanceOf[SentimentLine].sentiment).map {
      case (key, sentimentList) => (key, sentimentList.map(_.asInstanceOf[SentimentLine].msgNum).sum)
    }.map {
      case (x, y) => new SentimentLine(x, y)
    }
    val s = SocialData("all datasources", k.toList)
    logger.info(s"------------->  ${s.datasource} ")
    DataResponse(200, "Coulio Bro!!!", s)

  }

  def f1(allSocialData: List[SocialData]) = {
    val mydata = allSocialData.map(_.data).flatten

    val k = (allSocialData.map(_.data).flatten).groupBy(_.asInstanceOf[SentimentLine].sentiment).map {
      case (key, sentimentList) => (key, sentimentList.map(_.asInstanceOf[SentimentLine].msgNum).sum)
    }.map {
      case (x, y) => new SentimentLine(x, y)
    }
    val s = SocialData("all datasources", k.toList)
     DataResponse(200, "Coulio Bro!!!", s)

  }

}


object SentimentDataFuture {

  def getFuture(implicit ctx: ExecutionContext): Future[List[Int]] = Future {
    getData
  }

  def getSent()(implicit ctx: ExecutionContext): Future[List[Int]] = {
    val prom = Promise[List[Int]]()
    Future {
      prom.success(getData)
    }
    prom.future
  }


  def getData: List[Int] = {
    println("---------------> getData")
    List(1, 2, 3, 4, 5, 6)
  }
}
