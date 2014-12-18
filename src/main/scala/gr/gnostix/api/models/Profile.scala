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
                   language: String,
                    vierasTotalRating: Double)

object ProfileDao extends DatabaseAccessSupport {

  val logger = LoggerFactory.getLogger(getClass)

  implicit val getProfileResult = GetResult(r => Profile(r.<<, r.<<, r.<<, r.<<,
    r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

  def findById(profileId: Int, userId: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[Profile]( s""" select c.profile_id, c.profile_firstname,
                     c.registration_date,c.email,c.userlevel,c.total_counts,c.enabled,c.total_keywords,c.language,c.VIERAS_TOTAL_RATING
                              from profiles c
                            where c.profile_id = $profileId  and c.fk_user_id = $userId """)
        records.list()

    }
  }

  def getAllProfiles(userId: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[Profile]( s"""select c.profile_id, c.profile_name,
                       c.registration_date,c.email,c.userlevel,c.total_counts,c.enabled,c.total_keywords,c.language,c.VIERAS_TOTAL_RATING
                                from profiles c
                              where  c.fk_user_id = $userId """)
        records.list
    }
  }

  def updateProfileName(userId: Int, profileId: Int, profileName: String): Option[Boolean] = {
    getConnection withSession {
      implicit session =>
        try {
          Q.updateNA(
            s""" update profiles set profile_name = '${profileName}' where profile_id = ${profileId}
               and fk_user_id = $userId """).execute()
          Some(true)
        } catch {
          case e: Exception => {
            e.printStackTrace()
            None
          }
        }
    }
  }


  def createProfile(userId: Int, profileName: String): Option[Int] = {
    getConnection withSession {
      implicit session =>
        try {
          Q.u( s""" insert into profiles (profile_id, profile_name, fk_user_id) values (SEQ_PROFILE_ID.nextval, '${profileName}', $userId) """).execute()

          val profileId = Q.queryNA[Int]( s""" select * from (
                                    select c.profile_id from profiles c where c.fk_user_id = $userId order by c.profile_id desc
                                  ) where rownum = 1 """)
          if (profileId.list().size > 0) {
            Some(profileId.first())
          } else {
            None
          }

        } catch {
          case e: Exception => {
            e.printStackTrace()
            None
          }
        }

    }
  }

  def deleteProfile(userId: Int, profileId: Int): Option[Boolean] = {
    getConnection withSession {
      implicit session =>
        try {
          Q.u( s"""delete from profiles c
                              where  c.fk_user_id = $userId and profile_id = ${profileId}""").execute()
          Some(true)
        } catch {
          case e: Exception => {
            e.printStackTrace()
            None
          }
        }
    }
  }


}