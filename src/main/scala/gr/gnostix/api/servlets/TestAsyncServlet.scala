
package gr.gnostix.api.servlets

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import gr.gnostix.api.auth.AuthenticationSupport
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json.JacksonJsonSupport
import org.scalatra.{Accepted, CorsSupport, FutureSupport, ScalatraServlet}

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._


case class TestAsyncServlet(system: ActorSystem, myActor: ActorRef) extends ScalatraServlet
with JacksonJsonSupport
with AuthenticationSupport
with CorsSupport
with FutureSupport {

  options("/*") {
    response.setHeader("Access-Control-Allow-Headers", request.getHeader("Access-Control-Request-Headers"))
  }

  // Sets up automatic case class to JSON output serialization, required by
  // the JValueResult trait.
  protected implicit val jsonFormats: Formats = DefaultFormats
  //val db: Database

  implicit val timeout = new Timeout(2 seconds)
  protected implicit def executor: ExecutionContext = system.dispatcher

  before() {
    contentType = formats("json")
  }

   get("/async") {
    val future: Future[Any] =  myActor ? "sentiment"

     val ko = future.onSuccess{
       case x: String => {
         logger.info(s"------------->  ${x}")
         x
       }
     }
     //logger.info(s"------------->  ${ko}")
     ko
    //    logger.info(s"------------->  ${myActor}")
  }


}

//case class TestAsyncServlet(system: ActorSystem, myActor: ActorRef) extends GnostixAPIStack with TestAsyncDataRoutes
