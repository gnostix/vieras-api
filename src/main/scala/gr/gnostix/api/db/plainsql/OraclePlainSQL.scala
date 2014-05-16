package gr.gnostix.api.db.plainsql

//import slick.driver.JdbcDriver.backend.Database
import scala.slick.jdbc.JdbcBackend.Database
//import Database.dynamicSession
import scala.slick.jdbc.{GetResult, StaticQuery => Q}
import java.sql.Date
import gr.gnostix.api.models._

trait OraclePlainSQLQueries {

	val db: Database
	case class BetaUser(id: Int, firstName: String, lastName: String, company: String,
											email: String, newsletter: Int, signupDate: Date, expirationDate: Date)


	implicit val getBetaUserResult = GetResult(r => BetaUser(r.<<, r.<<, r.<<, r.<<,
		r.<<, r.<<, r.<<, r.<<))


	def getBetaUsers = {
		db withSession {
			implicit session =>
				val records = Q.queryNA[BetaUser]("select * from BETA_TESTING")/* foreach {
					u =>
						println("------>" + u.email)
				}*/
				records.list()
		}
	}

/*  def authUser(username: String, password: String): Option[User] = {
    db withSession {
      implicit session =>
        val records = Q.queryNA[User]("select * from users where username = '"
          + username + "' and password = '" + password +"'")

        records
    }
  }*/

}

