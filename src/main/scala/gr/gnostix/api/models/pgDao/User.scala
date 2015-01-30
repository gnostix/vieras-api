package gr.gnostix.api.models.pgDao


import java.sql.Timestamp

import gr.gnostix.api.db.plainsql.{DatabaseAccessSupportPg}
import gr.gnostix.api.models.plainModels.Payload
import org.slf4j.LoggerFactory

import scala.slick.jdbc.{GetResult, StaticQuery => Q}


case class User(userId: Int, username: String, var password: String,
                userLevel: Int,
                userDetails: UserDetails,
                userTotals: UserTotals) extends Payload

case class UserDetails(firstName: String, lastName: String,
                       registrationDate: Timestamp, email: String, streetAddress: String,
                       streetNum: String, postalCode: String, city: String,
                       companyName: String,
                       language: String,
                       expirationDate: Timestamp)

case class UserTotals(totalCounts: Int, totalKeywords: Int,
                      enabled: Int, totalProfiles: Int,
                      totalTopicProfiles: Int, totalSocialAccounts: Int,
                      totalHotelAccounts: Int)

object UserDao extends DatabaseAccessSupportPg {

  val logger = LoggerFactory.getLogger(getClass)

  implicit val getUserResult = GetResult(r => User(r.<<, r.<<, r.<<, r.<<,
    UserDetails(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<),
    UserTotals(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)))

  def findById(userId: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[User]( s"""
          select id, username, password, userlevel, user_firstname, user_lastname, registration_date,
            email, street_address, street_no, postal_code, city, company, language, expiration_date,
            total_counts, total_keywords, enabled, total_profiles, total_topic_profiles,
            total_social_account, total_hotels
          from vieras.users where id =  $userId
          """)
        records.first
    }
  }

  def findByUsername(username: String): Option[User] = {
    getConnection withSession {
      implicit session =>

        try {
          val records = Q.queryNA[User]( s"""
        select id, username, password, userlevel, user_firstname, user_lastname, registration_date,
            email, street_address, street_no, postal_code, city, company, language, expiration_date,
            total_counts, total_keywords, enabled, total_profiles, total_topic_profiles,
            total_social_account, total_hotels
          from vieras.users where username = '$username'
          """)
          if (records.list.size == 0) None else Some(records.first)
        } catch {
          case e: Exception => e.printStackTrace()
            None
        }

    }
  }

    def getUsers = {
      getConnection withSession {
        implicit session =>
          val records = Q.queryNA[Int]( """select count(*) from vieras.users""")
          records.first
      }
    }


  }

