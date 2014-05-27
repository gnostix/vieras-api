package gr.gnostix.api.db.plainsql

//import slick.driver.JdbcDriver.backend.Database
import scala.slick.jdbc.JdbcBackend.Database
//import Database.dynamicSession
import scala.slick.jdbc.{GetResult, StaticQuery => Q}
import java.sql.Date
import gr.gnostix.api.models._

trait OraclePlainSQLQueries {

	val db: Database


/*
	implicit val getUserResult = GetResult(r => User1(r.<<, r.<<, r.<<, r.<<,
    UserDetails(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<),
		UserTotals(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)))

	def getUsers = {
		db withSession {
			implicit session =>
				val records = Q.queryNA[User1]("select * from USERS")/* foreach {
					u =>
						println("------>" + u.email)
				}*/
				records.list()
		}
	}*/

/*  def authUser(username: String, password: String): Option[User] = {
    db withSession {
      implicit session =>
        val records = Q.queryNA[User]("select * from users where username = '"
          + username + "' and password = '" + password +"'")

        records
    }
  }*/

}
