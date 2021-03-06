package gr.gnostix.api

import org.scalatra.test.specs2._
import gr.gnostix.api.servlets.MyScalatraServlet

// For more on Specs2, see http://etorreborre.github.com/specs2/guide/org.specs2.guide.QuickStart.html
class MyScalatraServletSpec extends ScalatraSpec { def is =
  "GET /api/testtheapi on MyScalatraServlet"                     ^
    "should return status 200"                  ! root200^
                                                end

  addServlet(classOf[MyScalatraServlet], "/api/*")

  def root200 = get("/testtheapi") {
    status must_== 200
  }
}
