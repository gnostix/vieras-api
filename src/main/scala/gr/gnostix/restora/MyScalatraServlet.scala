package gr.gnostix.restora

import org.scalatra._
import scalate.ScalateSupport
import gr.gnostix.restora.db.plainsql.OraclePlainSQLQueries
import scala.slick.jdbc.JdbcBackend.Database



trait RestApiRoutes extends ScalatraServlet with OraclePlainSQLQueries {

    get("/betausers"){
      getBetaUsers.toString()
    }

}

case class MyScalatraServlet(db: Database) extends ScalatraoraStack with RestApiRoutes

