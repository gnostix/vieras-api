package gr.gnostix.restora.db.plainsql

//import slick.driver.JdbcDriver.backend.Database
import scala.slick.jdbc.JdbcBackend.Database
//import Database.dynamicSession
import scala.slick.jdbc.{GetResult, StaticQuery => Q}
import java.sql.Date

trait OraclePlainSQLQueries {

	val db: Database
	case class BetaUser(id: Int, firstName: String, lastName: String, company: String,
											email: String, newsletter: Int, signupDate: Date, expirationDate: Date)


	implicit val getBetaUserResult = GetResult(r => BetaUser(r.<<, r.<<, r.<<, r.<<,
		r.<<, r.<<, r.<<, r.<<))


	def getBetaUsers = {
		db withSession {
			implicit session =>
				Q.queryNA[BetaUser]("select * from BETA_TESTING") foreach {
					u =>
						println("------>" + u.email)
				}
		}
	}


}

