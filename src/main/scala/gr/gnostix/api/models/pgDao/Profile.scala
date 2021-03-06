package gr.gnostix.api.models.pgDao

import java.sql.Timestamp

import scala.slick.jdbc.{GetResult, StaticQuery => Q}

import gr.gnostix.api.db.plainsql.DatabaseAccessSupportPg
import gr.gnostix.api.models.plainModels.ApiData
import org.slf4j.LoggerFactory

case class Profile(profileId: Int,
                   profileName: String,
                   creationDate: Timestamp,
                   email: String,
                   profileLevel: Int,
                   enabled: Int,
                   language: String,
                   var vierasTotalRating: Double,
                   var companies: List[CompanyGroup] = List())

object ProfileDao extends DatabaseAccessSupportPg {

  val logger = LoggerFactory.getLogger(getClass)

  implicit val getProfileResult = GetResult(r => Profile(r.<<, r.<<, r.<<, r.<<,
    r.<<, r.<<, r.<<, r.<<))

  def findById(userId: Int, profileId: Int): Option[ApiData] = {
    getConnection withSession {
      implicit session =>

        try {
          val records = Q.queryNA[Profile]( s"""
                select c.id, c.profile_name,c.registration_date,c.email,c.userlevel,c.enabled,
                    c.language,c.VIERAS_TOTAL_RATING
                   from vieras.profiles c  where c.id = $profileId
          """)
          //  and c.fk_user_id = $userId  -- I need to add this but first make corectly the auth with basic auth and api key. otherwise
          // as it is now the user after basic auth doesnt own a userId !!!
          val profiles = records.list()

          profiles.map {
            x => {
              val rating = Q.queryNA[(Int, Double)](
                s"""
                      select r.fk_hotel_id ,max(r.vieras_total_rating)
                          from vieras.eng_hotel_stats r, vieras.ENG_PROFILE_HOTEL_CREDENTIALS f
                          where r.fk_hotel_id = f.fk_hotel_id
                                and f.FK_HOTEL_ID IN (
                            SELECT FK_HOTEL_ID FROM vieras.ENG_PROFILE_HOTEL_CREDENTIALS cr, vieras.eng_company co
                            WHERE cr.FK_company_ID = co.id and co.type = 'MYCOMPANY'
                                  and co.fk_profile_id=${x.profileId}
                          )
                          and r.CREATED between now()::timestamp(0) - interval '30' day and now()::timestamp(0)
                                and vieras_total_rating is not null
                          group by r.fk_hotel_id ,r.vieras_total_rating
           """).list()

              if (rating.size > 0) {
                BigDecimal(rating.map(x => x._2).sum / rating.size).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
              } else 0

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

  def getProfilesByUserId(userId: Int): Option[List[Profile]] = {
    getConnection withSession {
      implicit session =>

        try {
          val records = Q.queryNA[Profile](
            s"""
          select c.id, c.profile_name,  c.registration_date, c.email,c.userlevel,
              c.enabled,c.language,c.VIERAS_TOTAL_RATING
            from vieras.profiles c  where  c.fk_user_id = $userId
          """)
          val profiles = records.list()

          profiles.map {
            x => {
              x.vierasTotalRating = {
                val rating = Q.queryNA[(Int, Double)](
                  s"""
                      select r.fk_hotel_id ,max(r.vieras_total_rating)
                          from vieras.eng_hotel_stats r, vieras.ENG_PROFILE_HOTEL_CREDENTIALS f
                          where r.fk_hotel_id = f.fk_hotel_id
                                and f.FK_HOTEL_ID IN (
                            SELECT FK_HOTEL_ID FROM vieras.ENG_PROFILE_HOTEL_CREDENTIALS cr, vieras.eng_company co
                            WHERE cr.FK_company_ID = co.id and co.type = 'MYCOMPANY'
                                  and co.fk_profile_id=${x.profileId}
                          )
                          and r.CREATED between now()::timestamp(0) - interval '30' day and now()::timestamp(0)
                                and vieras_total_rating is not null
                          group by r.fk_hotel_id ,r.vieras_total_rating
           """).list()

                if (rating.size > 0) {
                  BigDecimal(rating.map(x => x._2).sum / rating.size).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
                } else 0
              }

              x.companies = CompanyDao.getCompaniesByProfileUserId(userId, x.profileId)
            }
          }
          Some(profiles)
        }
    }

  }


  def getAllProfilesAndCompanies(userId: Int): Option[List[ApiData]] = {
    getConnection withSession {
      implicit session =>

        try {
          val records = Q.queryNA[Profile](
            s"""
          select c.id, c.profile_name,  c.registration_date, c.email,c.userlevel,
              c.enabled,c.language,c.VIERAS_TOTAL_RATING
            from vieras.profiles c  where  c.fk_user_id = $userId
          """)
          val profiles = records.list()

          profiles.map {
            x => {
              x.vierasTotalRating = {
                val rating = Q.queryNA[(Int, Double)](
                  s"""
                      select r.fk_hotel_id ,max(r.vieras_total_rating)
                          from vieras.eng_hotel_stats r, vieras.ENG_PROFILE_HOTEL_CREDENTIALS f
                          where r.fk_hotel_id = f.fk_hotel_id
                                and f.FK_HOTEL_ID IN (
                            SELECT FK_HOTEL_ID FROM vieras.ENG_PROFILE_HOTEL_CREDENTIALS cr, vieras.eng_company co
                            WHERE cr.FK_company_ID = co.id and co.type = 'MYCOMPANY'
                                  and co.fk_profile_id=${x.profileId}
                          )
                          and r.CREATED between now()::timestamp(0) - interval '30' day and now()::timestamp(0)
                                and vieras_total_rating is not null
                          group by r.fk_hotel_id ,r.vieras_total_rating
           """).list()

                if (rating.size > 0) {
                  BigDecimal(rating.map(x => x._2).sum / rating.size).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
                } else 0
              }
            }
          }
          if (profiles.size > 0) {
            val companies: List[ApiData] = profiles.map { x => CompanyDao.findAllCompaniesAsApiData(userId, x.profileId).get} //.groupBy(x => x.dataName).toList

            val cleanCompanies = companies.groupBy(x => x.dataName).map {
              case (x, y) => ApiData(x, y.map(z => z.data.asInstanceOf[List[CompanyGroup]]).flatMap(f => f.map(g => g)))
            }

            logger.info("-----------> companies " + cleanCompanies)
            logger.info("-----------> profiles " + profiles)

            Some(List(ApiData("profiles", profiles.sortBy(_.profileId)), cleanCompanies.head))

          } else Some(List(ApiData("profiles", List()), ApiData("companies", List())))

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

          // update also the Company Group name
          CompanyDao.updateMyCompanyName(userId, profileId, profileName)

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
          val profileId = Q.queryNA[Int](
            s""" select vieras.create_profile(null,null,null,'${profileName}', null,$userId,2)
            """).list()

          if (profileId.size > 0) {
            Some(profileId.head)
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
          Q.updateNA( s"""delete from vieras.profiles c  where  c.fk_user_id = $userId and id = ${profileId}
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
