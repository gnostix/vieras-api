package gr.gnostix.api.servlets

import _root_.akka.actor.ActorSystem
import gr.gnostix.api.auth.AuthenticationSupport
import org.json4s.{DefaultFormats, Formats}
import org.scalatra._
import org.scalatra.json.JacksonJsonSupport

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Success, Failure}

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
