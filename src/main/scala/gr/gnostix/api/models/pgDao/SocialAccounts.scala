package gr.gnostix.api.models.pgDao

import java.sql.CallableStatement
import java.util.Calendar

import gr.gnostix.api.db.plainsql.DatabaseAccessSupportPg
import gr.gnostix.api.models.plainModels.{GoogleAnalyticsProfiles, DataGraph, SocialAccounts}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.io.Source
import scala.slick.jdbc.{GetResult, StaticQuery => Q}
import Q.interpolation

/**
 * Created by rebel on 4/8/14.
 */
case class SocialAccountsTwitter(credId: Int, handle: String, followers: Int,
                                 following: Int, listed: Int, statusNum: Int) extends DataGraph

case class SocialAccountsFacebook(credId: Int, fanPageFans: Int, talkingAboutCount: Int,
                                  talkingAboutSixDays: Int, checkins: Int, reach: Int, views: Int, engaged: Int, fanPage: String) extends DataGraph

case class SocialAccountsYoutube(credId: Int, subscribers: Int, views: Int, totalViews: Int, channelName: String) extends DataGraph

case class SocialAccountsGAnalytics(credId: Int, profileName: String, visits: Int, avgTimeOnSite: Int, newVisits: Int)
  extends DataGraph

case class SocialAccountsHotel(credId: Int, hotelId: Int, totalRating: Double, hotelName: String, hotelAddress: String,
                               hotelStars: Int, totalReviews: Int, hotelUrl: String, vierasTotalrating: Int) extends DataGraph

// social credentials
case class SocialCredentialsTw(token: String, tokenSecret: String, handle: String)

case class SocialCredentialsSimple(credentialsId: Int, accountName: String)

case class SocialCredentialsFb(token: String, fanpage: String, fanpageId: String, expires: java.util.Date)

case class SocialCredentialsYt(channelname: String, channelId: String, token: String)

//case class SocialCredentialsGa(gaAuthKey: String, gaName: String)

case class SocialCredentialsHotel(hotelUrl: String, dsId: Int)

case class UserHotelUrls(credId: Int, hotelUrl: String, dsId: Int)

case class SupportedHospitalitySites(name: String, id: Int)


object SocialAccountsTwitterDao extends DatabaseAccessSupportPg {
  val logger = LoggerFactory.getLogger(getClass)

  implicit val getSocialAccountsTwitterResult = GetResult(r => SocialAccountsTwitter(r.<<, r.<<,
    r.<<, r.<<, r.<<, r.<<))

  def findById(profileId: Int, credId: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[SocialAccountsTwitter](
          s"""
                select FK_PROFILE_SOCIAL_ENG_ID, HANDLE,max(FOLLOWERS) ,
                max(FOLLOWING),max(LISTED),max(STATUS_NUMBER) from vieras.eng_tw_stats, vieras.eng_engagement_data_queries i
                  where fk_eng_engagement_data_quer_id in (
                    select q.id from vieras.eng_engagement_data_queries q
                      where q.is_active = 1 and q.attr = 'TW_FFSL'
                        and FK_PROFILE_SOCIAL_ENG_ID = ${credId}
                        and FK_PROFILE_SOCIAL_ENG_ID in ( select s.id from vieras.eng_profile_social_credentials s
                         where s.fk_profile_id = ${profileId} and s.fk_datasource_id = 2)) and fk_eng_engagement_data_quer_id=i.id
                group by FK_PROFILE_SOCIAL_ENG_ID,handle
                """)
        val accounts = records.list()
        SocialAccounts("twitter", accounts)

    }
  }

  def getAllAccounts(implicit ctx: ExecutionContext, profileId: Int): Future[Option[SocialAccounts]] = {
    val prom = Promise[Option[SocialAccounts]]()

    Future {
      prom.success(
        getConnection withSession {
          implicit session =>
            try {
              val records = Q.queryNA[SocialAccountsTwitter](
                s"""
                select FK_PROFILE_SOCIAL_ENG_ID, HANDLE,max(FOLLOWERS) ,
                max(FOLLOWING),max(LISTED),max(STATUS_NUMBER) from vieras.eng_tw_stats, vieras.eng_engagement_data_queries i
                  where fk_eng_engagement_data_quer_id in (
                    select q.id from vieras.eng_engagement_data_queries q
                      where q.is_active = 1 and q.attr = 'TW_FFSL'
                        and FK_PROFILE_SOCIAL_ENG_ID in ( select s.id from vieras.eng_profile_social_credentials s
                         where s.fk_profile_id = ${profileId} and s.fk_datasource_id = 2)) and fk_eng_engagement_data_quer_id=i.id
                group by FK_PROFILE_SOCIAL_ENG_ID,handle
                 """)
              val accounts = records.list()
              if (accounts.isEmpty) {
                logger.info("-------------> the accounts is empty")
                None
              } else {
                Some(SocialAccounts("twitter", accounts))
              }

            } catch {
              case e: Exception => {
                e.printStackTrace()
                None
              }
            }
        })
    }
    prom.future
  }


  def addAccount(profileId: Int, token: String, tokenSecret: String, handle: String): Option[SocialCredentialsSimple] = {
    try {
      //CUSTOMERID in NUMBER, DATASOURCE IN VARCHAR2,
      //I_TOKEN IN VARCHAR2 , I_TOKENSECRET IN VARCHAR2 ,I_FBFANPAGE in VARCHAR2, I_FACEBOOK_EXPIRES_SEC IN NUMBER, FB_DATE_EXPIRES  IN DATE,
      //I_TWITTERHANDLE in VARCHAR2,I_YOUTUBE_USER in VARCHAR2,I_YOUTUBE_CHANNELID IN VARCHAR2,
      //I_G_ANALYTICS_AUTH_FILE in CLOB,I_GA_ACCOUNT_NAME in varchar2,  I_FANPAGE_ID  in varchar2, CREDENTIAL_ID OUT NUMBER

      val date = new java.util.Date();
      val sql: String = "{call vieras.insert_social_credential(?,?,?,?,?,?,?,?,?,?,?,?,?,?)}"
      val connection = getConnection.createConnection()
      val callableStatement: CallableStatement = connection.prepareCall(sql)
      callableStatement.setInt(1, profileId)
      callableStatement.setString(2, "TWITTER")
      callableStatement.setString(3, token)
      callableStatement.setString(4, tokenSecret)
      callableStatement.setString(5, "")
      callableStatement.setInt(6, 0)
      callableStatement.setDate(7, new java.sql.Date(date.getTime))
      callableStatement.setString(8, handle)
      callableStatement.setString(9, "")
      callableStatement.setString(10, "")
      callableStatement.setString(11, "")
      callableStatement.setString(12, "")
      callableStatement.setString(13, "")

      callableStatement.registerOutParameter(14, java.sql.Types.INTEGER)

      callableStatement.executeUpdate()

      val credId: Int = callableStatement.getInt(14)
      callableStatement.close()
      //connection.commit()
      connection.close()

      println("---------------------> " + credId)
      logger.info("---------->  addAccount twitter account " + credId)

      Some(SocialCredentialsSimple(credId, handle))

    } catch {
      case e: Exception => {
        logger.error("---------->  addAccount twitter account" + e.printStackTrace())
        None
      }
    }

  }
}

object SocialAccountsFacebookDao extends DatabaseAccessSupportPg {
  val logger = LoggerFactory.getLogger(getClass)

  implicit val getSocialAccountsFacebookResult = GetResult(r => SocialAccountsFacebook(r.<<, r.<<,
    r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

  def findById(profileId: Int, credId: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[SocialAccountsFacebook](
          s"""
          select FK_PROFILE_SOCIAL_ENG_ID ,max(fanpage_fans) , max(talking_about_count),max(talking_about_sixdays),
                 max(checkins), max(reach), max(views), max(engaged), max(fanpage)
          from vieras.eng_fb_stats ,vieras.eng_engagement_data_queries i
             where fk_eng_engagement_data_quer_id in (
                    select q.id from vieras.eng_engagement_data_queries q where q.is_active = 1 and q.attr = 'FB_FFSL'
                    and FK_PROFILE_SOCIAL_ENG_ID =  ${credId}
                    and FK_PROFILE_SOCIAL_ENG_ID in ( select s.id from vieras.eng_profile_social_credentials s
               where s.fk_profile_id = ${profileId} and s.fk_datasource_id = 1))
           and fk_eng_engagement_data_quer_id=i.id
          group by FK_PROFILE_SOCIAL_ENG_ID

   """)
        val accounts = records.list()
        SocialAccounts("facebook", accounts)

    }
  }

  def getAllAccounts(implicit ctx: ExecutionContext, profileId: Int): Future[Option[SocialAccounts]] = {
    val prom = Promise[Option[SocialAccounts]]()

    Future {
      prom.success(
        try {

          getConnection withSession {
            implicit session =>

              val records = Q.queryNA[SocialAccountsFacebook](
                s"""
          select FK_PROFILE_SOCIAL_ENG_ID ,max(fanpage_fans) , max(talking_about_count),max(talking_about_sixdays),
                 max(checkins), max(reach), max(views), max(engaged), max(fanpage)
          from vieras.eng_fb_stats ,vieras.eng_engagement_data_queries i
             where fk_eng_engagement_data_quer_id in (
                    select q.id from vieras.eng_engagement_data_queries q where q.is_active = 1 and q.attr = 'FB_FFSL'
                    and FK_PROFILE_SOCIAL_ENG_ID in ( select s.id from vieras.eng_profile_social_credentials s
               where s.fk_profile_id = ${profileId} and s.fk_datasource_id = 1))
           and fk_eng_engagement_data_quer_id=i.id
          group by FK_PROFILE_SOCIAL_ENG_ID
              """)
              val accounts = records.list()

              if (accounts.isEmpty) {
                None
              } else {
                Some(SocialAccounts("facebook", accounts))
              }
          }
        } catch {
          case e: Exception => {
            e.printStackTrace()
            None
          }
        })
    }
    prom.future
  }


  def addAccount(profileId: Int, cred: SocialCredentialsFb): Option[SocialCredentialsSimple] = {
    try {
      //CUSTOMERID in NUMBER, DATASOURCE IN VARCHAR2,
      //I_TOKEN IN VARCHAR2 , I_TOKENSECRET IN VARCHAR2 ,I_FBFANPAGE in VARCHAR2, I_FACEBOOK_EXPIRES_SEC IN NUMBER, FB_DATE_EXPIRES  IN DATE,
      //I_TWITTERHANDLE in VARCHAR2,I_YOUTUBE_USER in VARCHAR2,I_YOUTUBE_CHANNELID IN VARCHAR2,
      //I_G_ANALYTICS_AUTH_FILE in CLOB,I_GA_ACCOUNT_NAME in varchar2, I_FANPAGE_ID  in varchar2, CREDENTIAL_ID OUT NUMBER

      logger.info("---------->  addAccount facebook expireSec " + cred.expires)
      // sometime facebook doesn't provide expires date for token
      val tokenExpires = cred.expires match {
        case null => {
          val c: Calendar = Calendar.getInstance()
          c.add(Calendar.DAY_OF_YEAR, 5)
          val newdate = c.getTime
          newdate
        }
        case _ => cred.expires
      }

      val sql: String = "{call vieras.insert_social_credential(?,?,?,?,?,?,?,?,?,?,?,?,?,?)}"
      val connection = getConnection.createConnection()
      val callableStatement: CallableStatement = connection.prepareCall(sql)
      callableStatement.setInt(1, profileId)
      callableStatement.setString(2, "FACEBOOK")
      callableStatement.setString(3, cred.token)
      callableStatement.setString(4, "")
      callableStatement.setString(5, cred.fanpage)
      callableStatement.setInt(6, 409176) // expires in 10 days (we need this until we go to the new db Vieras)
      callableStatement.setDate(7, new java.sql.Date(tokenExpires.getTime))
      callableStatement.setString(8, "")
      callableStatement.setString(9, "")
      callableStatement.setString(10, "")
      callableStatement.setString(11, "")
      callableStatement.setString(12, "")
      callableStatement.setString(13, cred.fanpageId)

      callableStatement.registerOutParameter(14, java.sql.Types.INTEGER)

      callableStatement.executeUpdate()

      val credId: Int = callableStatement.getInt(14)
      callableStatement.close()
      //connection.commit()
      connection.close()

      println("---------------------> " + credId)
      logger.info("---------->  addAccount facebook account " + credId)

      Some(SocialCredentialsSimple(credId, cred.fanpage))

    } catch {
      case e: Exception => {
        logger.error("---------->  addAccount facebook account" + e.printStackTrace())
        None
      }
    }

  }


}

object SocialAccountsYoutubeDao extends DatabaseAccessSupportPg {
  val logger = LoggerFactory.getLogger(getClass)

  implicit val getSocialAccountsYoutubeResult = GetResult(r => SocialAccountsYoutube(r.<<, r.<<,
    r.<<, r.<<, r.<<))

  def findById(profileId: Int, credId: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[SocialAccountsYoutube](
          s"""
            select FK_PROFILE_SOCIAL_ENG_ID , max(subscribers) , max(video_views) , max(total_views), max(channel_name)
               from vieras.ENG_YT_STATS ,vieras.eng_engagement_data_queries i
               where fk_eng_engagement_data_quer_id in
                ( select q.id from vieras.eng_engagement_data_queries q
                    where q.is_active = 1 and  q.attr = 'YT_FFSL'
                    and FK_PROFILE_SOCIAL_ENG_ID = ${credId}
                        and FK_PROFILE_SOCIAL_ENG_ID in ( select s.id from vieras.eng_profile_social_credentials s
                         where s.fk_profile_id = ${profileId} and s.fk_datasource_id = 9)) and fk_eng_engagement_data_quer_id=i.id
              group by FK_PROFILE_SOCIAL_ENG_ID ,fk_eng_engagement_data_quer_id
              """)
        val accounts = records.list()
        SocialAccounts("youtube", accounts)

    }
  }

  def getAllAccounts(implicit ctx: ExecutionContext, profileId: Int): Future[Option[SocialAccounts]] = {
    val prom = Promise[Option[SocialAccounts]]()

    Future {
      prom.success(
        try {
          getConnection withSession {
            implicit session =>
              val records = Q.queryNA[SocialAccountsYoutube](
                s"""
            select FK_PROFILE_SOCIAL_ENG_ID , max(subscribers) , max(video_views) , max(total_views), max(channel_name)
               from vieras.ENG_YT_STATS ,vieras.eng_engagement_data_queries i
               where fk_eng_engagement_data_quer_id in
                ( select q.id from vieras.eng_engagement_data_queries q
                    where q.is_active = 1 and  q.attr = 'YT_FFSL'
                        and FK_PROFILE_SOCIAL_ENG_ID in ( select s.id from vieras.eng_profile_social_credentials s
                         where s.fk_profile_id = ${profileId} and s.fk_datasource_id = 9)) and fk_eng_engagement_data_quer_id=i.id
              group by FK_PROFILE_SOCIAL_ENG_ID ,fk_eng_engagement_data_quer_id
              """)
              val accounts = records.list()

              if (accounts.isEmpty) {
                None
              } else {
                Some(SocialAccounts("youtube", accounts))
              }
          }
        } catch {
          case e: Exception => {
            e.printStackTrace()
            None
          }
        }
      )
    }
    prom.future
  }

  def addAccount(profileId: Int, cred: SocialCredentialsYt): Option[SocialCredentialsSimple] = {
    try {
      //CUSTOMERID in NUMBER, DATASOURCE IN VARCHAR2,
      //I_TOKEN IN VARCHAR2 , I_TOKENSECRET IN VARCHAR2 ,I_FBFANPAGE in VARCHAR2, I_FACEBOOK_EXPIRES_SEC IN NUMBER, FB_DATE_EXPIRES  IN DATE,
      //I_TWITTERHANDLE in VARCHAR2,I_YOUTUBE_USER in VARCHAR2,I_YOUTUBE_CHANNELID IN VARCHAR2,
      //I_G_ANALYTICS_AUTH_FILE in CLOB,I_GA_ACCOUNT_NAME in varchar2,  I_FANPAGE_ID  in varchar2, CREDENTIAL_ID OUT NUMBER

      val date = new java.util.Date();
      val sql: String = "{call vieras.insert_social_credential(?,?,?,?,?,?,?,?,?,?,?,?,?,?)}"
      val connection = getConnection.createConnection()
      val callableStatement: CallableStatement = connection.prepareCall(sql)
      callableStatement.setInt(1, profileId)
      callableStatement.setString(2, "YOUTUBE")
      callableStatement.setString(3, cred.token)
      callableStatement.setString(4, "")
      callableStatement.setString(5, "")
      callableStatement.setInt(6, 0)
      callableStatement.setDate(7, new java.sql.Date(date.getTime))
      callableStatement.setString(8, "")
      callableStatement.setString(9, cred.channelname)
      callableStatement.setString(10, cred.channelId)
      callableStatement.setString(11, "")
      callableStatement.setString(12, "")
      callableStatement.setString(13, "")

      callableStatement.registerOutParameter(14, java.sql.Types.INTEGER)

      callableStatement.executeUpdate()

      val credId: Int = callableStatement.getInt(14)
      callableStatement.close()
      //connection.commit()
      connection.close()

      println("---------------------> " + credId)
      logger.info("---------->  addAccount youtube account " + credId)

      Some(SocialCredentialsSimple(credId, cred.channelname))

    } catch {
      case e: Exception => {
        logger.error("---------->  addAccount youtube account" + e.printStackTrace())
        None
      }
    }
  }


}

object SocialAccountsGAnalyticsDao extends DatabaseAccessSupportPg {
  val logger = LoggerFactory.getLogger(getClass)

  implicit val getSocialAccountsGAnalyticsResult = GetResult(r => SocialAccountsGAnalytics(r.<<, r.<<,
    r.<<, r.<<, r.<<))

  // Here we should bring data by browser or Operating system as extra analysis data

  def findById(profileId: Int, credId: Int) = {

    try {
      getConnection withSession {
        implicit session =>
          val records = Q.queryNA[SocialAccountsGAnalytics](
            s"""
                select FK_PROFILE_SOCIAL_ENG_ID, profile_name, max(visits),max(avgtimeonsite), max(newvisits)
                from vieras.eng_ga_stats, vieras.eng_engagement_data_queries i
                where  fk_eng_engagement_data_quer_id in (
                select q.id from vieras.eng_engagement_data_queries q
                  where q.is_active = 1 and q.attr = 'GA_STATS'
                  and FK_PROFILE_SOCIAL_ENG_ID = ${credId}
                        and FK_PROFILE_SOCIAL_ENG_ID in ( select s.id from vieras.eng_profile_social_credentials s
                         where s.fk_profile_id = ${profileId} and s.fk_datasource_id = 15)) and fk_eng_engagement_data_quer_id=i.id
                group by FK_PROFILE_SOCIAL_ENG_ID, profile_name
                """)
          val accounts = records.list()
          SocialAccounts("ganalytics", accounts)
      }

    } catch {
      case e: Exception => {
        e.printStackTrace()
        None
      }
    }

  }

  // this needs refactor
  def getAllAccounts(implicit ctx: ExecutionContext, profileId: Int): Future[Option[SocialAccounts]] = {
    val prom = Promise[Option[SocialAccounts]]()

    Future {
      prom.success(
        try {
          getConnection withSession {
            implicit session =>
              val records = Q.queryNA[SocialAccountsGAnalytics](
                s"""
                select FK_PROFILE_SOCIAL_ENG_ID, profile_name, max(users),max(avg_session_duration), max(new_users)
                from vieras.eng_ga_stats, vieras.eng_engagement_data_queries i
                where  fk_eng_engagement_data_quer_id in (
                select q.id from vieras.eng_engagement_data_queries q
                  where q.is_active = 1 and q.attr = 'GA_STATS'
                   and FK_PROFILE_SOCIAL_ENG_ID in ( select s.id from vieras.eng_profile_social_credentials s
                         where s.fk_profile_id = ${profileId} and s.fk_datasource_id = 15)) and fk_eng_engagement_data_quer_id=i.id
                group by FK_PROFILE_SOCIAL_ENG_ID, profile_name
                """)
              val accounts = records.list()

              if (accounts.isEmpty) {
                None
              } else {
                Some(SocialAccounts("ganalytics", accounts))
              }
          }
        } catch {
          case e: Exception => {
            e.printStackTrace()
            None
          }
        })
    }
    prom.future
  }

  // change the data for Google Analytics
  def addAccount(profileId: Int, token: String, refreshToken: String, cred: GoogleAnalyticsProfiles): Option[SocialCredentialsSimple] = {
    try {

      val date = new java.util.Date();
      val sql: String = "{call vieras.insert_social_credential(?,?,?,?,?,?,?,?,?,?,?,?,?,?)}"
      val connection = getConnection.createConnection()
      val callableStatement: CallableStatement = connection.prepareCall(sql)
      callableStatement.setInt(1, profileId)
      callableStatement.setString(2, "GOOGLEANALYTICS")
      callableStatement.setString(3, token)
      callableStatement.setString(4, refreshToken)
      callableStatement.setString(5, "")
      callableStatement.setInt(6, 0)
      callableStatement.setDate(7, new java.sql.Date(date.getTime))
      callableStatement.setString(8, "")
      callableStatement.setString(9, cred.accountId)
      callableStatement.setString(10, cred.webpropertyId)
      callableStatement.setString(11, cred.profileid)
      callableStatement.setString(12, cred.profileName)
      callableStatement.setString(13, "")

      callableStatement.registerOutParameter(14, java.sql.Types.INTEGER)

      callableStatement.executeUpdate()

      val credId: Int = callableStatement.getInt(14)
      callableStatement.close()
      //connection.commit()
      connection.close()

      println("---------------------> " + credId)
      logger.info("---------->  addAccount ganalytics account " + credId)

      Some(SocialCredentialsSimple(credId, cred.profileName))

    } catch {
      case e: Exception => {
        logger.error("---------->  addAccount ganalytics account" + e.printStackTrace())
        None
      }
    }
  }
}


object SocialAccountsHotelDao extends DatabaseAccessSupportPg {
  val logger = LoggerFactory.getLogger(getClass)

  implicit val getSocialAccountsHotelResult = GetResult(r => SocialAccountsHotel(r.<<, r.<<,
    r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))
  implicit val getHotelUrsResult = GetResult(r => UserHotelUrls(r.<<, r.<<, r.<<))
  implicit val getHospitalitylUrsResult = GetResult(r => SupportedHospitalitySites(r.<<, r.<<))


  def findById(profileId: Int, credId: Int) = {
    try {
      getConnection withSession {
        implicit session =>
          val records = Q.queryNA[SocialAccountsHotel](
            s"""
          select c.id, h.ID, h.TOTAL_RATING, h.HOTEL_NAME, h.HOTEL_ADDRESS,
                     h.HOTEL_STARS, h.TOTAL_REVIEWS, h.HOTEL_URL, h.VIERAS_TOTAL_RATING
                   from vieras.eng_hotels h, vieras.ENG_PROFILE_HOTEL_CREDENTIALS c
                     where h.id = c.fk_hotel_id
                      and c.FK_PROFILE_id = ${profileId}
                      and c.id = ${credId}
                   order by h.TOTAL_RATING desc
                   """)
          val accounts = records.list()
          SocialAccounts("hotel", accounts)

      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        None
      }
    }

  }

  def getAllAccounts(implicit ctx: ExecutionContext, profileId: Int): Future[Option[SocialAccounts]] = {
    val prom = Promise[Option[SocialAccounts]]()

    Future {
      prom.success(
        try {
          getConnection withSession {
            implicit session =>
              val records = Q.queryNA[SocialAccountsHotel](
                s"""
          select c.id, h.ID, h.TOTAL_RATING, h.HOTEL_NAME, h.HOTEL_ADDRESS,
                     h.HOTEL_STARS, h.TOTAL_REVIEWS, h.HOTEL_URL, h.VIERAS_TOTAL_RATING
                   from vieras.eng_hotels h, vieras.ENG_PROFILE_HOTEL_CREDENTIALS c
                     where h.id = c.fk_hotel_id
                      and c.FK_PROFILE_id = ${profileId}
                   order by h.TOTAL_RATING desc
                   """)
              val accounts = records.list()

              if (accounts.isEmpty) {
                None
              } else {
                Some(SocialAccounts("hotel", accounts))
              }
          }
        } catch {
          case e: Exception => {
            e.printStackTrace()
            None
          }
        }
      )
    }
    prom.future
  }


  def addAccount(profileId: Int, cred: SocialCredentialsHotel, datasourceName: String): Option[Int] = {
    try {

      val sql: String = "{call vieras.insert_hotel_credential(?,?,?,?,?)}"
      val connection = getConnection.createConnection()
      val callableStatement: CallableStatement = connection.prepareCall(sql)
      callableStatement.setInt(1, profileId)
      callableStatement.setInt(2, cred.dsId)
      if (cred.hotelUrl.startsWith("http://")) {
        callableStatement.setString(3, cred.hotelUrl)
      } else {
        callableStatement.setString(3, "http://" + cred.hotelUrl)
      }
      callableStatement.setString(4, datasourceName)
      callableStatement.registerOutParameter(5, java.sql.Types.INTEGER)

      callableStatement.executeUpdate()

      val hotelId: Int = callableStatement.getInt(5)
      callableStatement.close()
      //connection.commit()
      connection.close()

      println("---------------------> " + hotelId)
      logger.info("---------->  addAccount hotel url " + hotelId)

      Some(hotelId)

    } catch {
      case e: Exception => {
        logger.error("---------->  addAccount hotel url " + e.printStackTrace())
        None
      }
    }

  }


  def checkHotelUrl(url: String): (String, Boolean, String) = {

    // check if the url is real
    if (!checkIfUrlIsvalid(url)) {
      ("bad url", false, "")
    } else {

      // check if the user has already entered this url
      if (checkUrlInDatabase(url)) {
        ("You have already entered this url", false, "")
      } else {

        // check if the url source contains the supported datasources
        if (!checkUrlForSupportedHospitalityUrl(url)) {
          ("This url is not supported", false, "")
        } else {
          ("Good url", true, getUrlDomain(url))
        }

      }

    }

  }

  private def getUrlDomain(url: String): String ={
    if(url.startsWith("http://www."))
      url.split("\\.").toList.tail.head.capitalize
    else if (url.startsWith("http://"))
      url.drop(7).split("\\.").toList.head.capitalize
    else if (url.startsWith("www."))
      url.split("\\.").toList.tail.head.capitalize
    else ""

  }

  def checkIfUrlIsvalid(url: String): Boolean = {
    try {
      if (url.startsWith("http://")) {
        Source.fromURL(url)
        true
      } else {
        Source.fromURL("http://" + url)
        true
      }
    } catch {
      case e: Exception => logger.error("---------->  bad url " + e.printStackTrace())
        false
    }

  }

  def checkUrlInDatabase(url: String): Boolean = {
    try {
      getConnection withSession {
        implicit session =>
          val newUrl = if (url.startsWith("http://")) {
            url
          } else {
            "http://" + url
          }

          val credId = Q.queryNA[Int]( s"""
             select i.id from  vieras.ENG_PROFILE_HOTEL_CREDENTIALS i,vieras.eng_hotels h
                  where i.fk_hotel_id=h.id

                    and h.hotel_url = '$newUrl'
                    """).list()
          //and fk_profile_id = $profileId
          logger.error("---------->    credId credId credId " + credId.size)

          if (credId.size > 0) {
            true
          } else {
            false
          }
      }

    } catch {
      case e: Exception => logger.error("---------->  bad url " + e.printStackTrace())
        false
    }

  }

  def checkUrlForSupportedHospitalityUrl(url: String): Boolean = {
    try {
      getConnection withSession {
        implicit session =>
          val credId = Q.queryNA[String]( s"""  select ds_name from vieras.vieras_datasources where fk_g_id=9  """)
          val hospitalityUrls = credId.list()

          val isSupported = hospitalityUrls.map(ds_name => url.toLowerCase.contains(ds_name.toLowerCase))
          logger.error("---------->  isSupported   isSupported " + isSupported)
          if (isSupported.contains(true)) {
            true
          } else {
            logger.error("---------->  not supported   isSupported ")
            false
          }
      }

    } catch {
      case e: Exception => logger.error("---------->  bad url " + e.printStackTrace())
        false
    }

  }


  def getHospitalitySites() = {
    try {
      getConnection withSession {
        implicit session =>
          val credId = Q.queryNA[SupportedHospitalitySites]( s""" select ds_name, id from vieras.vieras_datasources where fk_g_id=9 """)
          credId.list()
      }

    } catch {
      case e: Exception => logger.error("---------->  bad url " + e.printStackTrace())
    }

  }

  def getHotelUrls(profileId: Int) = {
    try {
      getConnection withSession {
        implicit session =>
          val urls = Q.queryNA[UserHotelUrls]( s"""
                  select i.id,h.hotel_url,i.fk_datasource_id
                   from vieras.ENG_PROFILE_HOTEL_CREDENTIALS i, vieras.eng_hotels h
                       where i.fk_hotel_id=h.id
                    and I.FK_PROFILE_id = $profileId
            """)
          urls.list()
      }

    } catch {
      case e: Exception => logger.error("---------->  bad url " + e.printStackTrace())
    }
  }

  object SocialAccountsQueriesDao extends DatabaseAccessSupportPg {

    def deleteSocialAccount(profileId: Int, credId: Int, datasource: String) = Option[Int] {
      try {

        val sql = datasource match {
          case "hotel" => "{call vieras.delete_hotel_credential(?, ?)}"
          case "twitter" | "facebook" | "youtube" | "ganalytics" => "{call vieras.delete_social_credential(?, ?)}"
        }

        val connection = getConnection.createConnection()
        val callableStatement: CallableStatement = connection.prepareCall(sql)
        callableStatement.setInt(1, profileId)
        callableStatement.setInt(2, credId)

        callableStatement.executeUpdate()

        callableStatement.close()
        //connection.commit()
        connection.close()

        val status: Int = 200
        status

      } catch {
        case e: Exception => {
          logger.error("---------->  delete social account error " + e.printStackTrace())
          val status: Int = 400
          status
        }
      }
    }

  }

}
