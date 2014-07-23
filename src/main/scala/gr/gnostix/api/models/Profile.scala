package gr.gnostix.api.models

import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import org.slf4j.LoggerFactory
import scala.slick.jdbc.{GetResult, StaticQuery => Q}
import java.sql.Timestamp


case class Profile(profileId: Int,
                   profileName: String,
                   creationDate: Timestamp,
                   email: String,
                   profileLevel: Int,
                   totalCounts: Int,
                   enabled: Int,
                   totalKeywords: Int,
                   language: String)

object ProfileDao extends DatabaseAccessSupport {

  val logger = LoggerFactory.getLogger(getClass)

  implicit val getProfileResult = GetResult(r => Profile(r.<<, r.<<, r.<<, r.<<,
    r.<<, r.<<, r.<<, r.<<, r.<<))

  def findById(profileId: Int, userId: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[Profile]( s"""select c.customer_id, c.customer_firstname,
          c.registration_date,c.email,c.userlevel,c.total_counts,c.enabled,c.total_keywords,c.language
          from customers c, user_customer_map m, users u
          where c.customer_id = m.customer_id and u.user_id = m.user_id
            and u.user_id = $userId and c.customer_id = $profileId""")
        records.list()

    }
  }

  def getAllProfiles(userId: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[Profile]( s"""select c.customer_id, c.customer_firstname,
          c.registration_date,c.email,c.userlevel,c.total_counts,c.enabled,c.total_keywords,c.language
          from customers c, user_customer_map m, users u
          where c.customer_id = m.customer_id and u.user_id = m.user_id and u.user_id = $userId """)
        records.list
    }
  }

}