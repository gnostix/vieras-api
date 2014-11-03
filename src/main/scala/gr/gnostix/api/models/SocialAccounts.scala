package gr.gnostix.api.models


import java.sql.{Date, CallableStatement}

import gr.gnostix.api.db.plainsql.DatabaseAccessSupport

import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory

import scala.concurrent.{Future, Promise, ExecutionContext}
import scala.io.Source
import scala.slick.jdbc.{StaticQuery => Q, GetResult}
import Q.interpolation

/**
 * Created by rebel on 4/8/14.
 */
case class SocialAccountsTwitter(credId: Int, handle: String, followers: Int,
                                 following: Int, listed: Int, statusNum: Int) extends DataGraph

case class SocialAccountsFacebook(credId: Int, fanPageFans: Int, friends: Int, talkingAboutCount: Int,
                                  talkingAboutSixDays: Int, checkins: Int, reach: Int, fanPage: String) extends DataGraph

case class SocialAccountsYoutube(credId: Int, subscribers: Int, views: Int, totalViews: Int, channelName: String) extends DataGraph

case class SocialAccountsGAnalytics(credId: Int, profileName: String, visits: Int, avgTimeOnSite: Int, newVisits: Int)
  extends DataGraph

case class SocialAccountsHotel(credId: Int, hotelId: Int, totalRating: Double, hotelName: String, hotelAddress: String,
                               hotelStars: Int, totalReviews: Int, hotelUrl: String) extends DataGraph

// social credentials
case class SocialCredentialsTw(token: String, tokenSecret: String, handle: String)

case class SocialCredentialsSimple(credentialsId: Int, accountName: String)

case class SocialCredentialsFb(token: String, fanpage: String, validated: java.util.Date, expireSec: Int)

case class SocialCredentialsYt(channelname: String, channelId: String)

case class SocialCredentialsGa(gaAuthKey: String, gaName: String)

case class SocialCredentialsHotel(hotelUrl: String, dsId: Int)

case class UserHotelUrls(credId: Int, hotelUrl: String, dsId: Int)

case class SupportedHospitalitySites(name: String, id: Int)


object SocialAccountsTwitterDao extends DatabaseAccessSupport {
  val logger = LoggerFactory.getLogger(getClass)

  implicit val getSocialAccountsTwitterResult = GetResult(r => SocialAccountsTwitter(r.<<, r.<<,
    r.<<, r.<<, r.<<, r.<<))

  def findById(profileId: Int, credId: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[SocialAccountsTwitter](
          s""" select fk_cust_social_engagement_id, HANDLE,max(FOLLOWERS) ,
                max(FOLLOWING),max(LISTED),max(STATUS_NUMBER) from eng_tw_stats,eng_engagement_data_queries i
                  where fk_eng_engagement_data_quer_id in ( select q.id from eng_engagement_data_queries q
                      where q.is_active = 1 and q.attr = 'TW_FFSL'
                        and fk_cust_social_engagement_id = $credId
                        and fk_cust_social_engagement_id in ( select s.id from eng_cust_social_credentials s
                         where s.fk_cust_id = $profileId and s.fk_datasource_id = 2)) and fk_eng_engagement_data_quer_id=i.id
                group by fk_cust_social_engagement_id,handle """)
        val accounts = records.list()
        SocialAccounts("twitter", accounts)

    }
  }

  def getAllAccounts(implicit ctx: ExecutionContext, profileId: Int) = {
    val prom = Promise[SocialAccounts]()

    Future {
      prom.success(
        getConnection withSession {
          implicit session =>
            val records = Q.queryNA[SocialAccountsTwitter](
              s"""select fk_cust_social_engagement_id, HANDLE,max(FOLLOWERS) ,
                max(FOLLOWING),max(LISTED),max(STATUS_NUMBER) from eng_tw_stats,eng_engagement_data_queries i
                  where fk_eng_engagement_data_quer_id in ( select q.id from eng_engagement_data_queries q
                      where q.is_active = 1 and q.attr = 'TW_FFSL'
                        and fk_cust_social_engagement_id in ( select s.id from eng_cust_social_credentials s
                         where s.fk_cust_id = $profileId and s.fk_datasource_id = 2)) and fk_eng_engagement_data_quer_id=i.id
                group by fk_cust_social_engagement_id,handle """)
            val accounts = records.list()
            SocialAccounts("twitter", accounts)

        })
    }
    prom.future
  }


  def addAccount(profileId: Int, token: String, tokenSecret: String, handle: String): Option[SocialCredentialsSimple] = {
    try {
      //CUSTOMERID in NUMBER, DATASOURCE IN VARCHAR2,
      //I_TOKEN IN VARCHAR2 , I_TOKENSECRET IN VARCHAR2 ,I_FBFANPAGE in VARCHAR2, I_FACEBOOK_EXPIRES_SEC IN NUMBER, FB_DATE_EXPIRES  IN DATE,
      //I_TWITTERHANDLE in VARCHAR2,I_YOUTUBE_USER in VARCHAR2,I_YOUTUBE_CHANNELID IN VARCHAR2,
      //I_G_ANALYTICS_AUTH_FILE in CLOB,I_GA_ACCOUNT_NAME in varchar2, CREDENTIAL_ID OUT NUMBER

      val date = new java.util.Date();
      val sql: String = "{call PRC_INSERT_SOCIAL_CREDENTIAL(?,?,?,?,?,?,?,?,?,?,?,?,?)}"
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

      callableStatement.registerOutParameter(13, java.sql.Types.INTEGER)

      callableStatement.executeUpdate()

      val credId: Int = callableStatement.getInt(13)
      callableStatement.close()
      connection.commit()
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

object SocialAccountsFacebookDao extends DatabaseAccessSupport {
  val logger = LoggerFactory.getLogger(getClass)

  implicit val getSocialAccountsFacebookResult = GetResult(r => SocialAccountsFacebook(r.<<, r.<<,
    r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

  def findById(profileId: Int, credId: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[SocialAccountsFacebook](
          s""" select fk_cust_social_engagement_id ,max(fanpage_fans) , max(friend),
                max(talking_about_count),max(talking_about_sixdays),  max(checkins),max(reach), max(fanpage)
                  from eng_fb_stats ,eng_engagement_data_queries i
                   where fk_eng_engagement_data_quer_id in ( select q.id from
                      eng_engagement_data_queries q where q.is_active = 1 and q.attr = 'FB_FFSL'
                      and fk_cust_social_engagement_id = $credId
                      and fk_cust_social_engagement_id in ( select s.id from eng_cust_social_credentials s
                where s.fk_cust_id = $profileId and s.fk_datasource_id = 1)) and fk_eng_engagement_data_quer_id=i.id
              group by fk_cust_social_engagement_id """)
        val accounts = records.list()
        SocialAccounts("facebook", accounts)

    }
  }

  def getAllAccounts(implicit ctx: ExecutionContext, profileId: Int) = {
    val prom = Promise[SocialAccounts]()

    Future {
      prom.success(
        getConnection withSession {
          implicit session =>
            val records = Q.queryNA[SocialAccountsFacebook](
              s"""select fk_cust_social_engagement_id,max(fanpage_fans) , max(friend),
                max(talking_about_count),max(talking_about_sixdays),  max(checkins),max(reach), max(fanpage)
                  from eng_fb_stats ,eng_engagement_data_queries i
                   where fk_eng_engagement_data_quer_id in ( select q.id from
                      eng_engagement_data_queries q where q.is_active = 1 and q.attr = 'FB_FFSL'
                      and fk_cust_social_engagement_id in ( select s.id from eng_cust_social_credentials s
                where s.fk_cust_id = $profileId and s.fk_datasource_id = 1)) and fk_eng_engagement_data_quer_id=i.id
              group by fk_cust_social_engagement_id """)
            val accounts = records.list()
            SocialAccounts("facebook", accounts)

        })
    }
    prom.future
  }


  def addAccount(profileId: Int, cred: SocialCredentialsFb): Option[SocialCredentialsSimple] = {
    try {
      //CUSTOMERID in NUMBER, DATASOURCE IN VARCHAR2,
      //I_TOKEN IN VARCHAR2 , I_TOKENSECRET IN VARCHAR2 ,I_FBFANPAGE in VARCHAR2, I_FACEBOOK_EXPIRES_SEC IN NUMBER, FB_DATE_EXPIRES  IN DATE,
      //I_TWITTERHANDLE in VARCHAR2,I_YOUTUBE_USER in VARCHAR2,I_YOUTUBE_CHANNELID IN VARCHAR2,
      //I_G_ANALYTICS_AUTH_FILE in CLOB,I_GA_ACCOUNT_NAME in varchar2, CREDENTIAL_ID OUT NUMBER

      val date = new java.util.Date();
      val sql: String = "{call PRC_INSERT_SOCIAL_CREDENTIAL(?,?,?,?,?,?,?,?,?,?,?,?,?)}"
      val connection = getConnection.createConnection()
      val callableStatement: CallableStatement = connection.prepareCall(sql)
      callableStatement.setInt(1, profileId)
      callableStatement.setString(2, "FACEBOOK")
      callableStatement.setString(3, cred.token)
      callableStatement.setString(4, "")
      callableStatement.setString(5, cred.fanpage)
      callableStatement.setInt(6, cred.expireSec)
      callableStatement.setDate(7, new java.sql.Date(cred.validated.getTime))
      callableStatement.setString(8, "")
      callableStatement.setString(9, "")
      callableStatement.setString(10, "")
      callableStatement.setString(11, "")
      callableStatement.setString(12, "")

      callableStatement.registerOutParameter(13, java.sql.Types.INTEGER)

      callableStatement.executeUpdate()

      val credId: Int = callableStatement.getInt(13)
      callableStatement.close()
      connection.commit()
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

object SocialAccountsYoutubeDao extends DatabaseAccessSupport {
  val logger = LoggerFactory.getLogger(getClass)

  implicit val getSocialAccountsYoutubeResult = GetResult(r => SocialAccountsYoutube(r.<<, r.<<,
    r.<<, r.<<, r.<<))

  def findById(profileId: Int, credId: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[SocialAccountsYoutube](
          s"""select fk_cust_social_engagement_id , max(subscribers) , max(video_views) , max(total_views), max(channel_name)
               from ENG_YT_STATS ,eng_engagement_data_queries i
               where fk_eng_engagement_data_quer_id in
                ( select q.id from eng_engagement_data_queries q
                    where q.is_active = 1 and  q.attr = 'YT_FFSL'
                    and fk_cust_social_engagement_id = $credId
                    and fk_cust_social_engagement_id in
                      ( select s.id from eng_cust_social_credentials s where s.fk_cust_id = $profileId  and s.fk_datasource_id = 9))
                       and fk_eng_engagement_data_quer_id=i.id
              group by fk_cust_social_engagement_id ,fk_eng_engagement_data_quer_id """)
        val accounts = records.list()
        SocialAccounts("youtube", accounts)

    }
  }

  def getAllAccounts(implicit ctx: ExecutionContext, profileId: Int) = {
    val prom = Promise[SocialAccounts]()

    Future {
      prom.success(
        getConnection withSession {
          implicit session =>
            val records = Q.queryNA[SocialAccountsYoutube](
              s"""select fk_cust_social_engagement_id ,max(subscribers) , max(video_views) , max(total_views), max(channel_name)
               from ENG_YT_STATS ,eng_engagement_data_queries i
               where fk_eng_engagement_data_quer_id in
                ( select q.id from eng_engagement_data_queries q
                    where q.is_active = 1 and  q.attr = 'YT_FFSL'
                    and fk_cust_social_engagement_id in
                      ( select s.id from eng_cust_social_credentials s where s.fk_cust_id = $profileId  and s.fk_datasource_id = 9))
                       and fk_eng_engagement_data_quer_id=i.id
              group by fk_cust_social_engagement_id ,fk_eng_engagement_data_quer_id """)
            val accounts = records.list()
            SocialAccounts("youtube", accounts)

        })
    }
    prom.future
  }

  def addAccount(profileId: Int, cred: SocialCredentialsYt): Option[SocialCredentialsSimple] = {
    try {
      //CUSTOMERID in NUMBER, DATASOURCE IN VARCHAR2,
      //I_TOKEN IN VARCHAR2 , I_TOKENSECRET IN VARCHAR2 ,I_FBFANPAGE in VARCHAR2, I_FACEBOOK_EXPIRES_SEC IN NUMBER, FB_DATE_EXPIRES  IN DATE,
      //I_TWITTERHANDLE in VARCHAR2,I_YOUTUBE_USER in VARCHAR2,I_YOUTUBE_CHANNELID IN VARCHAR2,
      //I_G_ANALYTICS_AUTH_FILE in CLOB,I_GA_ACCOUNT_NAME in varchar2, CREDENTIAL_ID OUT NUMBER

      val date = new java.util.Date();
      val sql: String = "{call PRC_INSERT_SOCIAL_CREDENTIAL(?,?,?,?,?,?,?,?,?,?,?,?,?)}"
      val connection = getConnection.createConnection()
      val callableStatement: CallableStatement = connection.prepareCall(sql)
      callableStatement.setInt(1, profileId)
      callableStatement.setString(2, "YOUTUBE")
      callableStatement.setString(3, "")
      callableStatement.setString(4, "")
      callableStatement.setString(5, "")
      callableStatement.setInt(6, 0)
      callableStatement.setDate(7, new java.sql.Date(date.getTime))
      callableStatement.setString(8, "")
      callableStatement.setString(9, cred.channelname)
      callableStatement.setString(10, cred.channelId)
      callableStatement.setString(11, "")
      callableStatement.setString(12, "")

      callableStatement.registerOutParameter(13, java.sql.Types.INTEGER)

      callableStatement.executeUpdate()

      val credId: Int = callableStatement.getInt(13)
      callableStatement.close()
      connection.commit()
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

object SocialAccountsGAnalyticsDao extends DatabaseAccessSupport {
  val logger = LoggerFactory.getLogger(getClass)

  implicit val getSocialAccountsGAnalyticsResult = GetResult(r => SocialAccountsGAnalytics(r.<<, r.<<,
    r.<<, r.<<, r.<<))

  // Here we should bring data by browser or Operating system as extra analysis data

  def findById(profileId: Int, queryId: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[SocialAccountsGAnalytics](
          s""" select fk_eng_engagement_data_quer_id, profile_name, max(visits),max(avgtimeonsite), max(newvisits)
                from eng_ga_stats where  fk_eng_engagement_data_quer_id = $queryId
                 and  fk_eng_engagement_data_quer_id in ( select q.id
                  from eng_engagement_data_queries q where q.is_active = 1 and q.attr = 'GA_STATS'
                  and fk_cust_social_engagement_id in ( select s.id from eng_cust_social_credentials s
                  where s.fk_cust_id = $profileId and s.fk_datasource_id = 15))
                group by fk_eng_engagement_data_quer_id, profile_name """)
        val accounts = records.list()
        SocialAccounts("ganalytics", accounts)

    }
  }

  def getAllAccounts(implicit ctx: ExecutionContext, profileId: Int) = {
    val prom = Promise[SocialAccounts]()

    Future {
      prom.success(
        getConnection withSession {
          implicit session =>
            val records = Q.queryNA[SocialAccountsGAnalytics](
              s""" select fk_eng_engagement_data_quer_id, profile_name, max(visits),max(avgtimeonsite), max(newvisits)
                from eng_ga_stats where fk_eng_engagement_data_quer_id in ( select q.id
                  from eng_engagement_data_queries q where q.is_active = 1 and q.attr = 'GA_STATS'
                  and fk_cust_social_engagement_id in ( select s.id from eng_cust_social_credentials s
                  where s.fk_cust_id = $profileId and s.fk_datasource_id = 15))
                group by fk_eng_engagement_data_quer_id, profile_name """)
            val accounts = records.list()
            SocialAccounts("ganalytics", accounts)

        })
    }
    prom.future
  }

  // change the data for Google Analytics
  def addAccount(profileId: Int, cred: SocialCredentialsGa): Option[SocialCredentialsSimple] = {
    try {
      //CUSTOMERID in NUMBER, DATASOURCE IN VARCHAR2,
      //I_TOKEN IN VARCHAR2 , I_TOKENSECRET IN VARCHAR2 ,I_FBFANPAGE in VARCHAR2, I_FACEBOOK_EXPIRES_SEC IN NUMBER, FB_DATE_EXPIRES  IN DATE,
      //I_TWITTERHANDLE in VARCHAR2,I_YOUTUBE_USER in VARCHAR2,I_YOUTUBE_CHANNELID IN VARCHAR2,
      //I_G_ANALYTICS_AUTH_FILE in CLOB,I_GA_ACCOUNT_NAME in varchar2, CREDENTIAL_ID OUT NUMBER

      val date = new java.util.Date();
      val sql: String = "{call PRC_INSERT_SOCIAL_CREDENTIAL(?,?,?,?,?,?,?,?,?,?,?,?,?)}"
      val connection = getConnection.createConnection()
      val callableStatement: CallableStatement = connection.prepareCall(sql)
      callableStatement.setInt(1, profileId)
      callableStatement.setString(2, "GOOGLEANALYTICS")
      callableStatement.setString(3, "")
      callableStatement.setString(4, "")
      callableStatement.setString(5, "")
      callableStatement.setInt(6, 0)
      callableStatement.setDate(7, new java.sql.Date(date.getTime))
      callableStatement.setString(8, "")
      callableStatement.setString(9, "")
      callableStatement.setString(10, "")
      callableStatement.setString(11, cred.gaAuthKey)
      callableStatement.setString(12, cred.gaName)

      callableStatement.registerOutParameter(13, java.sql.Types.INTEGER)

      callableStatement.executeUpdate()

      val credId: Int = callableStatement.getInt(13)
      callableStatement.close()
      connection.commit()
      connection.close()

      println("---------------------> " + credId)
      logger.info("---------->  addAccount youtube account " + credId)

      Some(SocialCredentialsSimple(credId, cred.gaName))

    } catch {
      case e: Exception => {
        logger.error("---------->  addAccount youtube account" + e.printStackTrace())
        None
      }
    }
  }
}


object SocialAccountsHotelDao extends DatabaseAccessSupport {
  val logger = LoggerFactory.getLogger(getClass)

  implicit val getSocialAccountsHotelResult = GetResult(r => SocialAccountsHotel(r.<<, r.<<,
    r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))
  implicit val getHotelUrsResult = GetResult(r => UserHotelUrls(r.<<, r.<<, r.<<))
  implicit val getHospitalitylUrsResult = GetResult(r => SupportedHospitalitySites(r.<<, r.<<))


  def findById(profileId: Int, queryId: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[SocialAccountsHotel](
          s"""select c.id, h.HOTEL_ID, h.TOTAL_RATING, h.HOTEL_NAME, h.HOTEL_ADDRESS,
                     h.HOTEL_STARS, h.TOTAL_REVIEWS, h.HOTEL_URL
                   from eng_hotels h, ENG_CUST_HOTEL_CREDENTIALS c
                     where h.hotel_id = c.fk_hotel_id
                      and c.fk_cust_id = $profileId
                      and c.id = $queryId
                   order by h.TOTAL_RATING desc """)
        val accounts = records.list()
        SocialAccounts("hotel", accounts)

    }
  }

  def getAllAccounts(implicit ctx: ExecutionContext, profileId: Int) = {
    val prom = Promise[SocialAccounts]()

    Future {
      prom.success(
        getConnection withSession {
          implicit session =>
            val records = Q.queryNA[SocialAccountsHotel](
              s""" select c.id, h.HOTEL_ID, h.TOTAL_RATING, h.HOTEL_NAME, h.HOTEL_ADDRESS,
                       h.HOTEL_STARS, h.TOTAL_REVIEWS, h.HOTEL_URL
                     from eng_hotels h, ENG_CUST_HOTEL_CREDENTIALS c
                       where h.hotel_id = c.fk_hotel_id
                        and c.fk_cust_id = $profileId
                     order by h.TOTAL_RATING desc """)
            val accounts = records.list()
            SocialAccounts("hotel", accounts)

        })
    }
    prom.future
  }


  def addAccount(profileId: Int, cred: SocialCredentialsHotel) = {
    try {

      val sql: String = "{call PRC_INSERT_HOTEL_CREDENTIAL(?,?,?,?)}"
      val connection = getConnection.createConnection()
      val callableStatement: CallableStatement = connection.prepareCall(sql)
      callableStatement.setInt(1, profileId)
      callableStatement.setInt(2, cred.dsId)
      if (cred.hotelUrl.startsWith("http://")) {
        callableStatement.setString(3, cred.hotelUrl)
      } else {
        callableStatement.setString(3, "http://" + cred.hotelUrl)
      }
      callableStatement.registerOutParameter(4, java.sql.Types.INTEGER)

      callableStatement.executeUpdate()

      val hotelId: Int = callableStatement.getInt(4)
      callableStatement.close()
      connection.commit()
      connection.close()

      println("---------------------> " + hotelId)
      logger.info("---------->  addAccount hotel url " + hotelId)

      hotelId

    } catch {
      case e: Exception => {
        logger.error("---------->  addAccount hotel url " + e.printStackTrace())
      }
    }

  }


  def checkHotelUrl(url: String): (String, Boolean) = {

    // check if the url is real
    if (!checkIfUrlIsvalid(url)) {
      ("bad url", false)
    } else {

      // check if the user has already entered this url
      if (checkUrlInDatabase(url)) {
        ("You have already entered this url", false)
      } else {

        // check if the url source contains the supported datasources
        if (!checkUrlForSupportedHospitalityUrl(url)) {
          ("This url is not supported", false)
        } else {
          ("Good url", true)
        }

      }

    }

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

          val credId = Q.queryNA[Int]( s""" select i.id from ENG_CUST_HOTEL_CREDENTIALS i,eng_hotels h
        where i.fk_hotel_id=h.hotel_id
          and I.FK_CUST_ID=10
          and h.hotel_url = '$newUrl' """).list()
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
          val credId = Q.queryNA[String]( s"""  select ds_name from vieras_datasources where fk_g_id=9  """)
          val hospitalityUrls = credId.list()

          val isSupported = hospitalityUrls.map(ds_name => url.contains(ds_name))
          logger.error("---------->  isSupported   isSupported " + isSupported)
          if (isSupported.contains("true")) {
            true
          } else {
            logger.error("---------->  not supported   isSupported " )
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
          val credId = Q.queryNA[SupportedHospitalitySites]( s""" select ds_name, ds_id from vieras_datasources where fk_g_id=9 """)
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
                                          select i.id,h.hotel_url,i.fk_datasource_id from ENG_CUST_HOTEL_CREDENTIALS i,eng_hotels h
                                                      where i.fk_hotel_id=h.hotel_id
                                                        and I.FK_CUST_ID=$profileId """)
          urls.list()
      }

    } catch {
      case e: Exception => logger.error("---------->  bad url " + e.printStackTrace())
    }
  }

  object SocialAccountsQueriesDao extends DatabaseAccessSupport {
    def deleteSocialAccount(profileId: Int, credId: Int, datasource: String) {
      try {
        getConnection withSession {
          implicit session =>
            datasource match {
              case "hotel" => (Q.u + s"""{call PRC_DELETE_HOTEL_CREDENTIAL($profileId, $credId)}""").execute()
              case "twitter" | "facebook" | "youtube" | "ganalytics" =>
                (Q.u + s"""{call PRC_DELETE_SOCIAL_CREDENTIAL($profileId, $credId)}""").execute()
            }
        }

      } catch {
        case e: Exception => logger.error("---------->  delete social account error " + e.printStackTrace())
      }
    }
  }

}