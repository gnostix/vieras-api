package gr.gnostix.api.models.pgDao

import gr.gnostix.api.db.plainsql.DatabaseAccessSupportPg
import gr.gnostix.api.models.plainModels.ApiData

import scala.slick.jdbc.{GetResult, StaticQuery => Q}

/**
  * Created by rebel on 19/3/15.
  */

case class HotelDatasourceData(datasource: String, url: String)

case class SocialDatasourceData(datasource: String, socialAccount: String)

case class CompanyGroup(companyGroupId: Int, companyGroupName: String, companyGroupType: String, profileId: Int,
                        var hospitality_data: List[HotelDatasourceData] = List(), var social_data: List[SocialDatasourceData] = List())

case class CompanyGroupJson(companyGroupName: String, companyGroupType: String)


object CompanyDao extends DatabaseAccessSupportPg {

  implicit val getProfileResult = GetResult(r => CompanyGroup(r.<<, r.<<, r.<<, r.<<))
  implicit val gethotelResult = GetResult(r => HotelDatasourceData(r.<<, r.<<))
  implicit val getSocialResult = GetResult(r => SocialDatasourceData(r.<<, r.<<))


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

          val companies = Q.queryNA[CompanyGroup](
            s""" select id, name, type, fk_profile_id from vieras.eng_company
               where fk_profile_id = ${profileId} and type='MYCOMPANY'
                and fk_profile_id in (select id from vieras.profiles where fk_user_id = ${userId})""").list()

          Some(ApiData("company", fillCompanyWithUrlAndSocialAccounts(companies).head))

        } catch {
          case e: Exception => e.printStackTrace()
            None
        }
    }
  }

  def findAllCompaniesAsApiData(userId: Int, profileId: Int): Option[ApiData] = {
    getConnection withSession {
      implicit session =>

        try {

          val companies = Q.queryNA[CompanyGroup](
            s""" select id, name, type, fk_profile_id from vieras.eng_company
               where fk_profile_id = ${profileId}
                and fk_profile_id in (select id from vieras.profiles where fk_user_id = ${userId})""").list()


          Some(ApiData("companies", fillCompanyWithUrlAndSocialAccounts(companies)))

        } catch {
          case e: Exception => e.printStackTrace()
            None
        }
    }
  }


  def getCompaniesByProfileUserId(userId: Int, profileId: Int): List[CompanyGroup] = {
    getConnection withSession {
      implicit session =>

        try {

          val companies = Q.queryNA[CompanyGroup](
            s""" select id, name, type, fk_profile_id from vieras.eng_company
               where fk_profile_id = ${profileId}
                and fk_profile_id in (select id from vieras.profiles where fk_user_id = ${userId})""").list()


          fillCompanyWithUrlAndSocialAccounts(companies)

        } catch {
          case e: Exception => e.printStackTrace()
            List()
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


  def createCompany(userId: Int, profileId: Int, company: CompanyGroupJson): Option[Int] = {
    getConnection withSession {
      implicit session =>

        try {
          (Q.u +
            s""" insert into vieras.eng_company (name, type, fk_profile_id)
                  values ('${company.companyGroupName}', '${company.companyGroupType}',$profileId)
            """).execute()

          val companyId = Q.queryNA[Int](
            s""" select * from (  select c.id from vieras.eng_company c where
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
          Q.updateNA(
            s"""delete from vieras.eng_company  where  fk_profile_id = ${profileId} and id = ${companyId}
                   and fk_profile_id in (select id from vieras.profiles where fk_user_id = ${userId})
            """).execute()

          Some(true)

        } catch {
          case e: Exception => e.printStackTrace()
            None
        }
    }
  }

  def fillCompanyWithUrlAndSocialAccounts(companies: List[CompanyGroup]): List[CompanyGroup] = {
    getConnection withSession {
      implicit session =>

        companies.foreach {
          co => {
            co.hospitality_data = getCompanyHotelUrls(co)
            co.social_data = getCompanySocialAccounts(co)
          }
        }
        try {


          companies
        } catch {
          case e: Exception => e.printStackTrace()
            List()
        }
    }
  }

  def getCompanyHotelUrls(company: CompanyGroup): List[HotelDatasourceData] = {
    getConnection withSession {
      implicit session =>

        try {

          val hotelDatasourceData = Q.queryNA[HotelDatasourceData](
            s""" select h.datasource_name as datasource, h.hotel_url from vieras.eng_hotels h, vieras.eng_profile_hotel_credentials cr
               where h.id = cr.fk_hotel_id and fk_company_id = ${company.companyGroupId};
              """).list()

          hotelDatasourceData
        } catch {
          case e: Exception => e.printStackTrace()
            List()
        }
    }
  }

  def getCompanySocialAccounts(company: CompanyGroup): List[SocialDatasourceData] = {
    getConnection withSession {
      implicit session =>

        try {

          val socialDatasourceData = Q.queryNA[SocialDatasourceData](
            s""" select d.ds_name as datasource, coalesce(youtube_user, twitter_handle, fb_fan_page, ga_profile_name) social_account_name
                	from vieras.eng_profile_social_credentials cr, vieras.eng_datasources d
                	where cr.fk_datasource_id = d.id and fk_company_id = ${company.companyGroupId};
              """).list()

          socialDatasourceData
        } catch {
          case e: Exception => e.printStackTrace()
            List()
        }
    }
  }
}
