package gr.gnostix.api.models.pgDao

import gr.gnostix.api.db.plainsql.DatabaseAccessSupportPg
import gr.gnostix.api.models.plainModels.ApiData

import scala.slick.jdbc.{GetResult, StaticQuery => Q}

/**
 * Created by rebel on 19/3/15.
 */

case class CompanyGroup(companyGroupId: Int, companyGroupName: String, companyGroupType: String, profileId: Int)

object CompanyDao extends DatabaseAccessSupportPg {

  implicit val getProfileResult = GetResult(r => CompanyGroup(r.<<, r.<<, r.<<, r.<<))


  def findById(userId: Int, profileId: Int, companyId: Int): Option[ApiData] = {
    getConnection withSession {
      implicit session =>

        try {

          val company = Q.queryNA[CompanyGroup](
            s""" select id, name, type, fk_profile_id from vieras.eng_company
               where fk_profile_id = ${profileId}
                and fk_profile_id in (select id from vieras.profiles where fk_user_id = ${userId})""").list()


          Some(ApiData("company", company.head))

        } catch {
          case e: Exception => e.printStackTrace()
            None
        }
    }
  }

  def getMyCompany(userId: Int, profileId: Int): Option[ApiData] = {
    getConnection withSession {
      implicit session =>

        try {

          val company = Q.queryNA[CompanyGroup](
            s""" select id, name, type, fk_profile_id from vieras.eng_company
               where fk_profile_id = ${profileId} and type='MYCOMPANY'
                and fk_profile_id in (select id from vieras.profiles where fk_user_id = ${userId})""").list()


          Some(ApiData("company", company.head))

        } catch {
          case e: Exception => e.printStackTrace()
            None
        }
    }
  }

  def findAllCompanies(userId: Int, profileId: Int): Option[ApiData] = {
    getConnection withSession {
      implicit session =>

        try {

          val company = Q.queryNA[CompanyGroup](
            s""" select id, name, type, fk_profile_id from vieras.eng_company
               where fk_profile_id = ${profileId}
                and fk_profile_id in (select id from vieras.profiles where fk_user_id = ${userId})""").list()


          Some(ApiData("companies", company))

        } catch {
          case e: Exception => e.printStackTrace()
            None
        }
    }
  }

  def updateName(userId: Int, profileId: Int, companyId: Int, companyName: String): Option[Boolean] = {
    getConnection withSession {
      implicit session =>

        try {

          // update also the company group name to be the same as the profile name
          Q.updateNA(
            s""" update vieras.eng_company set name = '${companyName}'
                 where id = ${companyId} and fk_profile_id = ${profileId}
                 and fk_profile_id in (select id from vieras.profiles where fk_user_id = ${userId})
             """).execute()
          Some(true)

        } catch {
          case e: Exception => e.printStackTrace()
            None
        }
    }
  }

  def updateMyCompanyName(userId: Int, profileId: Int, companyName: String): Option[Boolean] = {
    getConnection withSession {
      implicit session =>

        try {

          // update also the company group name to be the same as the profile name
          Q.updateNA(
            s""" update vieras.eng_company set name = '${companyName}'
                 where fk_profile_id = ${profileId} and type = 'MYCOMPANY'
                 and fk_profile_id in (select id from vieras.profiles where fk_user_id = ${userId})
             """).execute()
          Some(true)

        } catch {
          case e: Exception => e.printStackTrace()
            None
        }
    }
  }


  def createCompany(profileId: Int, company: CompanyGroup): Option[Int] = {
    getConnection withSession {
      implicit session =>

        try {

          // create also a company group
          Q.u(
            s""" insert into vieras.eng_company (name, type, fk_profile_id)
                  values ('${company.companyGroupName}', '${company.companyGroupType}',$profileId)
            """).execute()

          val companyId = Q.queryNA[Int]( s""" select * from (  select c.id from vieras.eng_company c where
                                                c.fk_profile_id = $profileId order by c.id desc) as foo
                                                limit 1
                                          """)

          if (companyId.list().size > 0) {
            Some(companyId.first())
          } else {
            None
          }

        } catch {
          case e: Exception => e.printStackTrace()
            None
        }
    }
  }

  def deleteCompany(userId: Int, profileId: Int, companyId: Int): Option[Boolean] = {
    getConnection withSession {
      implicit session =>

        try {

          // delete  from company groups
          Q.u( s"""delete from vieras.eng_company  where  fk_profile_id = ${profileId} and id = ${companyId}
                   and fk_profile_id in (select id from vieras.profiles where fk_user_id = ${userId})
            """).execute()

          Some(true)

        } catch {
          case e: Exception => e.printStackTrace()
            None
        }
    }
  }


}
