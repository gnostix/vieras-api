package gr.gnostix.api.models

import java.sql.Timestamp
import scala.slick.jdbc.JdbcBackend._
import scala.slick.jdbc.{StaticQuery, GetResult}
import scala.slick.jdbc.{GetResult, StaticQuery => Q}
import gr.gnostix.api.db.plainsql.DatabaseAccessSupport


case class User1(userId: Int, username: String, password: String,
                 userLevel: Int,
                 userDetails: UserDetails,
                 userTotals: UserTotals)

case class UserDetails1(firstName: String, lastName: String,
                        registrationDate: Timestamp, email: String, streetAddress: String,
                        streetNum: String, postalCode: String, city: String,
                        companyName: String,
                        language: String,
                        expirationDate: Timestamp)

case class UserTotals1(totalCounts: Int, totalKeywords: Int,
                       enabled: Int, sentEmail: Int,
                       totalProfiles: Int, totalFbFanPages: Int,
                       totalTwitterAccounts: Int, totalTopicProfiles: String)

object UserDao1 extends DatabaseAccessSupport{

  implicit val getUserResult = GetResult(r => User(r.<<, r.<<, r.<<, r.<<,
    UserDetails(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<),
    UserTotals(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)))


  def getUsers1() = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[User]("select * from USERS")
        records.list()
    }
  }

  def getUsers2(db: Database) = {
    db withSession {
      implicit session =>
        val records = Q.queryNA[User]("select * from USERS")
        records.list()
    }
  }

}

