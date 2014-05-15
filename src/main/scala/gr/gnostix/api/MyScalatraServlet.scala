package gr.gnostix.api

import org.scalatra._
import scalate.ScalateSupport
import gr.gnostix.api.db.plainsql.OraclePlainSQLQueries
import scala.slick.jdbc.JdbcBackend.Database
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json._




trait RestApiRoutes extends ScalatraServlet with OraclePlainSQLQueries with JacksonJsonSupport {

	// Sets up automatic case class to JSON output serialization, required by
	// the JValueResult trait.
	protected implicit val jsonFormats: Formats = DefaultFormats

	before() {
		contentType = formats("json")
	}
    get("/betausers"){
      getBetaUsers
    }

}

case class MyScalatraServlet(db: Database) extends GnostixAPIStack with RestApiRoutes

