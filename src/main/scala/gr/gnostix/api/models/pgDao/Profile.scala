package gr.gnostix.api.models.pgDao

import java.sql.Timestamp
import java.text.DecimalFormat

import gr.gnostix.api.db.plainsql.DatabaseAccessSupportPg
import gr.gnostix.api.models.plainModels.{ApiMessages, ApiData}
import org.slf4j.LoggerFactory

import scala.slick.jdbc.{GetResult, StaticQuery => Q}


case class Profile(profileId: Int,
                   profileName: String,
                   creationDate: Timestamp,
                   email: String,
                   profileLevel: Int,
                   totalCounts: Int,
                   enabled: Int,
                   totalKeywords: Int,
                   language: String,
                   var vierasTotalRating: Double)

object ProfileDao extends DatabaseAccessSupportPg {

  val logger = LoggerFactory.getLogger(getClass)

  implicit val getProfileResult = GetResult(r => Profile(r.<<, r.<<, r.<<, r.<<,
    r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

  def findById(profileId: Int, userId: Int): Option[ApiData] = {
    getConnection withSession {
      implicit session =>

        try {
          val records = Q.queryNA[Profile]( s"""
                select c.id, c.profile_firstname,c.registration_date,c.email,c.userlevel,c.total_counts,c.enabled,
                    c.total_keywords,c.language,c.VIERAS_TOTAL_RATING
                   from vieras.profiles c  where c.id = $profileId  and c.fk_user_id = $userId
          """)
          val profiles = records.list()

          profiles.map {
            x => x.vierasTotalRating = {
              val rating = Q.queryNA[Double](
                s"""
          select vieras_total_rating from vieras.eng_hotels where vieras_total_rating is not null
	          and id in (select fk_hotel_id from vieras.eng_profile_hotel_credentials where fk_profile_id=${x.profileId})
           """).list()

              // in order to get the double with 2 digits precision instead of 5.23455 we get 5.34
              BigDecimal(rating.sum / rating.size).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
            }
          }

          if (profiles.size > 0) {
            Some(ApiData("profile", profiles.head))
          } else Some(ApiData("nodata", profiles))

        } catch {
          case e: Exception => e.printStackTrace()
            None
        }
    }
  }


  def getAllProfiles(userId: Int) = {
    getConnection withSession {
      implicit session =>

        try {
          val records = Q.queryNA[Profile]( s"""
          select c.id, c.profile_name,  c.registration_date, c.email,c.userlevel,
              c.total_counts,c.enabled,c.total_keywords,c.language,c.VIERAS_TOTAL_RATING
            from vieras.profiles c  where  c.fk_user_id = $userId
          """)
          val profiles = records.list()

          profiles.map {
            x => x.vierasTotalRating = {
              val rating = Q.queryNA[Double](
                s"""
          select vieras_total_rating from vieras.eng_hotels where vieras_total_rating is not null
	          and id in (select fk_hotel_id from vieras.eng_profile_hotel_credentials where fk_profile_id=${x.profileId})
           """).list()

              if (rating.size > 0) {
                BigDecimal(rating.sum / rating.size).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
              } else 0
            }
          }

          if (profiles.size > 0) {
            Some(ApiData("profiles", profiles))
          } else Some(ApiData("nodata", profiles))

        } catch {
          case e: Exception => e.printStackTrace()
            None
        }
    }
  }

  def updateProfileName(userId: Int, profileId: Int, profileName: String): Option[Boolean] = {
    getConnection withSession {
      implicit session =>
        try {
          Q.updateNA(
            s""" update vieras.profiles set profile_name = '${profileName}' where id = ${profileId}
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
          Q.u(
            s""" insert into vieras.profiles (profile_name, fk_user_id)
                  values ('${profileName}', $userId)
            """).execute()

          val profileId = Q.queryNA[Int]( s""" select * from (  select c.id from vieras.profiles c where
                                                c.fk_user_id = 2 order by c.id desc) as foo
                                                limit 1
                                          """)
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
          Q.u( s"""delete from profiles c  where  c.fk_user_id = $userId and id = ${profileId}
            """).execute()
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
