package gr.gnostix.api.models

import java.sql.Timestamp
import gr.gnostix.api.db.plainsql.OraclePlainSQLQueries

import scala.slick.jdbc.JdbcBackend.Database
import javax.sql.DataSource
import com.mchange.v2.c3p0.ComboPooledDataSource

//import Database.dynamicSession

import scala.slick.jdbc.{GetResult, StaticQuery => Q}


case class User(userId: Int, username: String, password: String,
                userLevel: Int,
                userDetails: UserDetails,
                userTotals: UserTotals)

case class UserDetails(firstName: String, lastName: String,
                       registrationDate: Timestamp, email: String, streetAddress: String,
                       streetNum: String, postalCode: String, city: String,
                       companyName: String,
                       language: String,
                       expirationDate: Timestamp)

case class UserTotals(totalCounts: Int, totalKeywords: Int,
                      enabled: Int, sentEmail: Int,
                      totalProfiles: Int, totalFbFanPages: Int,
                      totalTwitterAccounts: Int, totalTopicProfiles: String)

object UserDao {

  val db: Database = DB.database

  implicit val getUserResult = GetResult(r => User(r.<<, r.<<, r.<<, r.<<,
    UserDetails(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<),
    UserTotals(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)))

  def findById(userId: Int) = {
    db withSession {
      implicit session =>
        val records = Q.queryNA[User](s"select * from USERS where userId = $userId")
        records.first
    }
  }

  def findByUsername(username: String): Option[User] = {
    db withSession {
      implicit session =>
        val records = Q.queryNA[User](s"select * from USERS where username = $username")
        if (records.list.size == 0) None else Some(records.first)
    }

  }

  def blah() = {}

  def getUsers = {
    db withSession {
      implicit session =>
        val records = Q.queryNA[User]("select * from USERS") /* foreach {
					u =>
						println("------>" + u.email)
				}*/
        records.list()
    }
  }
}

  object DB {
    val database = {
      val ds = new ComboPooledDataSource
      ds.setDriverClass("oracle.jdbc.driver.OracleDriver")
      ds.setJdbcUrl("jdbc:oracle:thin:@db.gnstx.gr:1521:ora")
      ds.setUser("DUSR_BACKUP")
      ds.setPassword("sandripappa1977")
      ds.setMinPoolSize(1)
      ds.setAcquireIncrement(1)
      ds.setMaxPoolSize(5)
      ds.setInitialPoolSize(1)
      Database.forDataSource(ds)
    }
  }