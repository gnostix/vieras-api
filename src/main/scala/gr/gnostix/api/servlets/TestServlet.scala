package gr.gnostix.api.servlets

import gr.gnostix.api.GnostixAPIStack
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.ScalatraServlet
import org.scalatra.json.JacksonJsonSupport

/**
 * Created by rebel on 20/3/15.
 */
trait TestApiRoutes extends ScalatraServlet
with JacksonJsonSupport{
  // the JValueResult trait.
  protected implicit val jsonFormats: Formats = DefaultFormats

  before() {
    contentType = formats("json")
  }


  get("/test"){
    "works"
  }
}

case class TestServlet() extends GnostixAPIStack with TestApiRoutes