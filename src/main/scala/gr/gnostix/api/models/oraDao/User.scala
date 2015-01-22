package gr.gnostix.api.models.oraDao

import java.sql.Timestamp

import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
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
                      enabled: Int, sentEmail: Int,
                      totalProfiles: Int, totalFbFanPages: Int,
                      totalTwitterAccounts: Int, totalTopicProfiles: String,
                      totalYoutubeAccounts: Int, totalHotelAccounts: Int)

object UserDao extends DatabaseAccessSupport{

  val logger = LoggerFactory.getLogger(getClass)

  implicit val getUserResult = GetResult(r => User(r.<<, r.<<, r.<<, r.<<,
    UserDetails(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<),
    UserTotals(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)))

  def findById(userId: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[User](s"""select USER_ID, USERNAME, PASSWORD, USERLEVEL, USER_FIRSTNAME, USER_LASTNAME, REGISTRATION_DATE,
          EMAIL, STREET_ADDRESS, STREET_NO, POSTAL_CODE, CITY,COMPANY, LANGUAGE, EXPIRATION_DATE,
          TOTAL_COUNTS, TOTAL_KEYWORDS, ENABLED, SENT_EMAIL, TOTAL_PROFILES, TOTAL_FB_FAN_PAGES,
          TOTAL_TWITTER_ACCOUNTS,TOTAL_TOPIC_PROFILES, TOTAL_YOUTUBE_ACCOUNTS, TOTAL_HOTELS from USERS where USER_ID = $userId""")
        records.first
    }
  }

  def findByUsername(username: String): Option[User] = {
     getConnection withSession {
      implicit session =>
        val records = Q.queryNA[User](s"""select USER_ID, USERNAME, PASSWORD, USERLEVEL, USER_FIRSTNAME, USER_LASTNAME, REGISTRATION_DATE,
          EMAIL, STREET_ADDRESS, STREET_NO, POSTAL_CODE, CITY,COMPANY, LANGUAGE, EXPIRATION_DATE,
          TOTAL_COUNTS, TOTAL_KEYWORDS, ENABLED, SENT_EMAIL, TOTAL_PROFILES, TOTAL_FB_FAN_PAGES,
          TOTAL_TWITTER_ACCOUNTS,TOTAL_TOPIC_PROFILES, TOTAL_YOUTUBE_ACCOUNTS, TOTAL_HOTELS from USERS where username = '$username' """)
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


}
