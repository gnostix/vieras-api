package gr.gnostix.api.utilities

import java.sql.Timestamp

import gr.gnostix.api.db.plainsql.DatabaseAccessSupportPg
import oracle.sql.TIMESTAMPLTZ
import scala.slick.jdbc.{GetResult, StaticQuery => Q}


/**
  * Created by rebel on 16/6/14.
  */
object SqlUtils extends DatabaseAccessSupportPg {

  def buildHotelCredentialsQuery(userId: Int, profileId: Int, companyId: Int): String = {

    /* Profile with id = 0 means brind relevant data for all the profiles of this user.
     e.g. all hospitality data for his (company) hotel or all data for user hotels per datasource */
    val sql = profileId match {
      case 0 =>
        s"""
          SELECT FK_HOTEL_ID FROM vieras.ENG_PROFILE_HOTEL_CREDENTIALS cr, vieras.eng_company co, vieras.profiles p
            WHERE cr.FK_company_ID = co.id
              and co.type = 'MYCOMPANY'
              and co.fk_profile_id=p.id
              and p.fk_user_id = $userId
        """
      case _ =>
        s"""
          SELECT FK_HOTEL_ID FROM vieras.ENG_PROFILE_HOTEL_CREDENTIALS cr, vieras.eng_company co, vieras.profiles p
            WHERE cr.FK_company_ID = $companyId and cr.FK_company_ID = co.id
              and co.fk_profile_id = $profileId
              and co.fk_profile_id=p.id
              and p.fk_user_id = $userId
        """
    }

    sql
  }

  def buildHotelDatasourceQuery(userId: Int, profileId: Int, companyId: Int, datasourceId: Int): String = {
    val sql = profileId match {
      case 0 =>
        s"""
          SELECT FK_HOTEL_ID FROM vieras.ENG_PROFILE_HOTEL_CREDENTIALS cr, vieras.eng_company co, vieras.profiles p
            WHERE cr.FK_DATASOURCE_ID=${datasourceId}
              and cr.FK_company_ID = co.id
              and co.type = 'MYCOMPANY'
              and co.fk_profile_id=p.id
              and p.fk_user_id = $userId
        """
      case _ =>
        s"""
          SELECT FK_HOTEL_ID FROM vieras.ENG_PROFILE_HOTEL_CREDENTIALS cr, vieras.eng_company co, vieras.profiles p
            WHERE cr.FK_company_ID = $companyId and cr.FK_DATASOURCE_ID=${datasourceId} and cr.FK_company_ID = co.id
              and co.fk_profile_id=$profileId
              and co.fk_profile_id=p.id
              and p.fk_user_id = $userId
        """
    }

    sql
  }

  def buildHotelCredIdQuery(userId: Int, profileId: Int, companyId: Int, credId: Int): String = {
    val sql = profileId match {
      case 0 =>
        s"""
          SELECT FK_HOTEL_ID FROM vieras.ENG_PROFILE_HOTEL_CREDENTIALS cr, vieras.eng_company co, vieras.profiles p
            WHERE cr.ID=${credId}
             and cr.FK_company_ID = co.id
             and co.type = 'MYCOMPANY'
             and co.fk_profile_id=p.id
             and p.fk_user_id = $userId
        """
      case _ =>
        s"""
          SELECT FK_HOTEL_ID FROM vieras.ENG_PROFILE_HOTEL_CREDENTIALS cr, vieras.eng_company co, vieras.profiles p
            WHERE cr.FK_company_ID = $companyId and cr.ID=${credId} and cr.FK_company_ID = co.id
              and co.fk_profile_id=$profileId
              and co.fk_profile_id=p.id
              and p.fk_user_id = $userId
        """
    }
    sql

  }


  def buildSocialCredentialsQuery(userId: Int, profileId: Int, companyId: Int, datasourceName: String, credId: Option[Int]): String = {
    val sql = profileId match {
      case 0 =>
        s"""select s.id from vieras.eng_profile_social_credentials s , vieras.eng_company co, vieras.profiles p
              where s.fk_datasource_id in (select id from vieras.eng_datasources where LOWER(ds_name) = LOWER('${datasourceName}' ))
                and s.fk_company_id = co.id and co.fk_profile_id=p.id and p.fk_user_id = $userId)
        """
      case _ => {
        credId match {
          case Some(x) =>
            s"""select s.id from vieras.eng_profile_social_credentials s , vieras.eng_company co, vieras.profiles p
                    where s.id = $x
                      and s.fk_company_id in ( $companyId )
                      and s.fk_datasource_id in (select id from vieras.eng_datasources where LOWER(ds_name) = LOWER('${datasourceName}' ))
                      and s.fk_company_id = co.id and co.fk_profile_id = $profileId and co.fk_profile_id=p.id and p.fk_user_id = $userId)
        """
          case None =>
            s"""select s.id from vieras.eng_profile_social_credentials s , vieras.eng_company co, vieras.profiles p
                    where s.fk_company_id in ( $companyId )
                      and s.fk_datasource_id in (select id from vieras.eng_datasources where LOWER(ds_name) = LOWER('${datasourceName}' ))
                      and s.fk_company_id = co.id and co.fk_profile_id = $profileId and co.fk_profile_id=p.id and p.fk_user_id = $userId)
        """
        }
      }
    }

    sql
  }

  def getDataDefaultObj(profileId: Int): String = {
    val mySqlDyn = s"""fk_k_id in (select k_id from KEYWORDS where fk_TOPIC_id in (select sd_id from TOPICS where fk_profile_id=${profileId}))"""
    mySqlDyn
  }

  def getDataByKeywordsObj(userId: Int, profileId: Int, keywords: List[Int]): String = {
    val mySqlDyn = keywords match {
      case List() => s"""fk_k_id in (select k_id from KEYWORDS where fk_TOPIC_id in (select TOPIC_id from topics where fk_profile_id=${profileId}))"""
      case List(x) => s""" fk_k_id = ${keywords.head}"""
      case x :: xs => s"""fk_k_id in ( ${keywords.mkString(",")} )"""
    }
    mySqlDyn
  }

  def getDataByTopicsObj(userId: Int, profileId: Int, topics: List[Int]): String = {
    val mySqlDyn = topics match {
      case List() => s""" fk_k_id in (select k_id from KEYWORDS where fk_TOPIC_id in (select TOPIC_id from TOPICS where fk_profile_id=${profileId})) """
      case List(x) => s""" fk_k_id in (select k_id from KEYWORDS where fk_TOPIC_id = ${topics.head} ) """
      case x :: xs => s""" fk_k_id in (select k_id from KEYWORDS where fk_TOPIC_id in ( ${topics.mkString(",")} )) """
    }
    mySqlDyn
  }

  def logUserLogin(ipAddres: String, username: String, sessionId: String) = {
    getConnection withSession {
      implicit session =>
        try {
          Q.updateNA(
            s""" insert into vieras.user_checkins (token, user_ip, username, login_at)
                 values ('${sessionId}', '${ipAddres}', '${username}', now()::timestamp(0) )   """).execute()

        } catch {
          case e: Exception => {
            e.printStackTrace()
          }
        }
    }
  }

  def logUserLogout(username: String, sessionId: String) = {
    getConnection withSession {
      implicit session =>
        try {
          Q.updateNA(
            s""" update vieras.user_checkins  set logout_at = now()::timestamp(0)
                    where username = '${username}' and token = '${sessionId}'  """).execute()

        } catch {
          case e: Exception => {
            e.printStackTrace()
          }
        }
    }
  }


}
