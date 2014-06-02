package gr.gnostix.api.models

import java.sql.Timestamp
import gr.gnostix.api.db.plainsql.{DatabaseAccessSupport, DatabaseAccess, OraclePlainSQLQueries}

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

object UserDao extends DatabaseAccessSupport{

  implicit val getUserResult = GetResult(r => User(r.<<, r.<<, r.<<, r.<<,
    UserDetails(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<),
    UserTotals(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)))

  def findById(userId: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[User](s"select * from USERS where userId = $userId")
        records.first
    }
  }

  def findByUsername(username: String): Option[User] = {
    getConnection withSession {
      implicit session =>
        println("------------ findByUsername --------------")
        val records = Q.queryNA[User](s"select * from USERS where username = '$username'")
        println("----------> "  + Q.toString )
        if (records.list.size == 0) None else Some(records.first)
    }

  }

  def getUsers = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[User]("""select * from USERS""")
        records.list()
    }
  }

  def getJndi{
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[User]("select * from USERS")
        records.list()
    }

  }
}
