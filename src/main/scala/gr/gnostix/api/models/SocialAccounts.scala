package gr.gnostix.api.models

import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.concurrent.{Future, Promise, ExecutionContext}
import scala.slick.jdbc.{StaticQuery => Q, GetResult}


/**
 * Created by rebel on 4/8/14.
 */
case class SocialAccountsTwitter(queryId: Int, handle: String, followers: Int,
                                 following: Int, listed: Int, statusNum: Int) extends DataGraph

case class SocialAccountsFacebook(queryId: Int, fanpagefans: Int, friends: Int, talkingAboutCount: Int,
                                  talkingAboutSixDays: Int, checkins: Int, reach: Int) extends DataGraph

case class SocialAccountsYoutube(queryId: Int, subscribers: Int, views: Int, totalViews: Int) extends DataGraph

case class SocialAccountsGAnalytics(queryId: Int, profileName: String, visits: Int, avgTimeOnSite: Int, newVisits: Int)
  extends DataGraph

case class SocialAccountsHotel(queryId: Int, hotelId: Int, totalRating: Double, hotelName: String, hotelAddress: String,
                               hotelStars: Int, totalReviews: Int, hotelUrl: String) extends DataGraph

// social credntials
case class SocialCredentialsTw(token: String, tokenSecret: String, handle: String)

case class SocialCredentialsFb(token: String, fanpage: String, validated: DateTime, expireSec: Int)

case class SocialCredentialsYt(channelname: String, channelId: String)

case class SocialCredentialsGa(gaAuthKey: String, gaName: String)

case class SocialCredentialsHotel(hotelUrl: String, hotelDatasource: String, hotelName: String)


object SocialAccountsTwitterDao extends DatabaseAccessSupport {
  val logger = LoggerFactory.getLogger(getClass)

  implicit val getSocialAccountsTwitterResult = GetResult(r => SocialAccountsTwitter(r.<<, r.<<,
    r.<<, r.<<, r.<<, r.<<))

  def findById(profileId: Int, queryId: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[SocialAccountsTwitter](
          s"""select fk_eng_engagement_data_quer_id, HANDLE,max(FOLLOWERS) ,
                max(FOLLOWING),max(LISTED),max(STATUS_NUMBER) from eng_tw_stats
                  where fk_eng_engagement_data_quer_id = $queryId
                     and fk_eng_engagement_data_quer_id in ( select q.id from eng_engagement_data_queries q
                      where q.is_active = 1 and q.attr = 'TW_FFSL'
                        and fk_cust_social_engagement_id in ( select s.id from eng_cust_social_credentials s
                         where s.fk_cust_id = $profileId and s.fk_datasource_id = 2))
                group by fk_eng_engagement_data_quer_id,handle """)
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
              s"""select fk_eng_engagement_data_quer_id, HANDLE,max(FOLLOWERS) ,
                max(FOLLOWING),max(LISTED),max(STATUS_NUMBER) from eng_tw_stats
                  where fk_eng_engagement_data_quer_id in ( select q.id from eng_engagement_data_queries q
                      where q.is_active = 1 and q.attr = 'TW_FFSL'
                        and fk_cust_social_engagement_id in ( select s.id from eng_cust_social_credentials s
                         where s.fk_cust_id = $profileId and s.fk_datasource_id = 2))
                group by fk_eng_engagement_data_quer_id,handle """)
            val accounts = records.list()
            SocialAccounts("twitter", accounts)

        })
    }
    prom.future
  }

  def addAccount(profileId: Int, cred: SocialCredentialsTw) {
    getConnection withSession {
      var myId = 0
      implicit session =>
        try {

          //add account
          (Q.u + s"""insert into eng_cust_social_credentials (ID, TOKEN, TOKENSECRET, FK_DATASOURCE_ID, FK_CUST_ID, TWITTER_HANDLE)
          values (SEQ_ENG_CUST_SOCIAL_CREDENTIAL.nextval, '${cred.token}', '${cred.tokenSecret}', 2, $profileId , '${cred.handle}')""").execute()

          // get Id
          val result = Q.queryNA[Int]( s"""select id from eng_cust_social_credentials where TOKEN = '${cred.token}'
                                and TOKENSECRET = '${cred.tokenSecret}' and fk_cust_id = $profileId""")
          myId = result.first()
          SocialAccountsQueriesDao.insertQueries(myId, "twitter")
          logger.info("---------->  Id  $myId ")
          myId
        } catch {
          case e: Exception => {
            SocialAccountsQueriesDao.deleteSocialCredentialsExc(myId)
            logger.error("---------->  addAccount twitter " + e.printStackTrace())
          }
        }
    }
  }

}


object SocialAccountsFacebookDao extends DatabaseAccessSupport {
  val logger = LoggerFactory.getLogger(getClass)

  implicit val getSocialAccountsFacebookResult = GetResult(r => SocialAccountsFacebook(r.<<, r.<<,
    r.<<, r.<<, r.<<, r.<<, r.<<))

  def findById(profileId: Int, queryId: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[SocialAccountsFacebook](
          s"""select fk_eng_engagement_data_quer_id,max(fanpage_fans) , max(friend),
                max(talking_about_count),max(talking_about_sixdays),  max(checkins),max(reach)
                  from eng_fb_stats where fk_eng_engagement_data_quer_id = $queryId
                    and fk_eng_engagement_data_quer_id in ( select q.id from
                      eng_engagement_data_queries q where q.is_active = 1 and q.attr = 'FB_FFSL'
                      and fk_cust_social_engagement_id in ( select s.id from eng_cust_social_credentials s
                      where s.fk_cust_id = $profileId and s.fk_datasource_id = 1))
              group by fk_eng_engagement_data_quer_id """)
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
              s"""select fk_eng_engagement_data_quer_id,max(fanpage_fans) , max(friend),
                max(talking_about_count),max(talking_about_sixdays),  max(checkins),max(reach)
                  from eng_fb_stats where fk_eng_engagement_data_quer_id in ( select q.id from
                      eng_engagement_data_queries q where q.is_active = 1 and q.attr = 'FB_FFSL'
                      and fk_cust_social_engagement_id in ( select s.id from eng_cust_social_credentials s
                      where s.fk_cust_id = $profileId and s.fk_datasource_id = 1))
              group by fk_eng_engagement_data_quer_id """)
            val accounts = records.list()
            SocialAccounts("facebook", accounts)

        })
    }
    prom.future
  }

  def addAccount(profileId: Int, cred: SocialCredentialsFb) {
    getConnection withSession {
      var myId = 0
      implicit session =>
        try {

          //add account
          (Q.u + s"""insert into eng_cust_social_credentials (ID, FK_DATASOURCE_ID, FK_CUST_ID, VALIDATED)
          values (SEQ_ENG_CUST_SOCIAL_CREDENTIAL.nextval, '${cred.token}', '${cred.fanpage}', 1, $profileId , ${cred.validated}, ${cred.expireSec})""").execute()

          // get Id
          val result = Q.queryNA[Int]( s"""select id from eng_cust_social_credentials where TOKEN = '${cred.token}'
                                and FB_FAN_PAGE = '${cred.fanpage}' and fk_cust_id = $profileId""")
          myId = result.first()
          SocialAccountsQueriesDao.insertQueries(myId, "facebook")
          logger.info("---------->  Id  $myId ")
          myId
        } catch {
          case e: Exception => {
            SocialAccountsQueriesDao.deleteSocialCredentialsExc(myId)
            logger.error("---------->  addAccount twitter " + e.printStackTrace())
          }
        }
    }
  }


}

object SocialAccountsYoutubeDao extends DatabaseAccessSupport {
  val logger = LoggerFactory.getLogger(getClass)

  implicit val getSocialAccountsYoutubeResult = GetResult(r => SocialAccountsYoutube(r.<<, r.<<,
    r.<<, r.<<))

  def findById(profileId: Int, queryId: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[SocialAccountsYoutube](
          s"""select fk_eng_engagement_data_quer_id, max(subscribers) , max(video_views) , max(total_views)
               from ENG_YT_STATS where fk_eng_engagement_data_quer_id = $queryId
                 and  fk_eng_engagement_data_quer_id in
                ( select q.id from eng_engagement_data_queries q where q.is_active = 1 and
                q.attr = 'YT_FFSL' and fk_cust_social_engagement_id in ( select s.id
                  from eng_cust_social_credentials s where s.fk_cust_id = $profileId  and s.fk_datasource_id = 9))
              group by fk_eng_engagement_data_quer_id """)
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
              s"""select fk_eng_engagement_data_quer_id, max(subscribers) , max(video_views) , max(total_views)
               from ENG_YT_STATS where fk_eng_engagement_data_quer_id in
                ( select q.id from eng_engagement_data_queries q where q.is_active = 1 and
                q.attr = 'YT_FFSL' and fk_cust_social_engagement_id in ( select s.id
                  from eng_cust_social_credentials s where s.fk_cust_id = $profileId  and s.fk_datasource_id = 9))
              group by fk_eng_engagement_data_quer_id """)
            val accounts = records.list()
            SocialAccounts("youtube", accounts)

        })
    }
    prom.future
  }

  def addAccount(profileId: Int, cred: SocialCredentialsYt) {
    getConnection withSession {
      var myId = 0
      implicit session =>
        try {

          //add account
          (Q.u + s"""insert into eng_cust_social_credentials (ID, YOUTUBE_USER, FK_DATASOURCE_ID, FK_CUST_ID, YOUTUBE_CHANNELID)
          values (SEQ_ENG_CUST_SOCIAL_CREDENTIAL.nextval, '${cred.channelname}', 9, $profileId , '${cred.channelId}')""").execute()

          // get Id
          val result = Q.queryNA[Int](
            s"""select id from eng_cust_social_credentials
               where  YOUTUBE_USER = '${cred.channelname}' and fk_cust_id = $profileId""")
          myId = result.first()
          SocialAccountsQueriesDao.insertQueries(myId, "youtube")
          logger.info("---------->  Id  $myId ")
          myId
        } catch {
          case e: Exception => {
            SocialAccountsQueriesDao.deleteSocialCredentialsExc(myId)
            logger.error("---------->  addAccount youtube " + e.printStackTrace())
          }
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


  def addAccount(profileId: Int, cred: SocialCredentialsGa) {
    getConnection withSession {
      var myId = 0
      implicit session =>
        try {

          //add account
          (Q.u + s"""insert into eng_cust_social_credentials (ID, G_ANALYTICS_AUTH_FILE, FK_DATASOURCE_ID, FK_CUST_ID, GA_ACCOUNT_NAME)
          values (SEQ_ENG_CUST_SOCIAL_CREDENTIAL.nextval, '${cred.gaAuthKey}', 15, $profileId , '${cred.gaName}')""").execute()

          // get Id
          val result = Q.queryNA[Int](
            s"""select id from eng_cust_social_credentials
               where  G_ANALYTICS_AUTH_FILE = '${cred.gaAuthKey}' and fk_cust_id = $profileId""")
          myId = result.first()
          SocialAccountsQueriesDao.insertQueries(myId, "youtube")
          logger.info("---------->  Id  $myId ")
          myId
        } catch {
          case e: Exception => {
            SocialAccountsQueriesDao.deleteSocialCredentialsExc(myId)
            logger.error("---------->  addAccount ganalytics " + e.printStackTrace())
          }
        }
    }
  }

}


object SocialAccountsHotelDao extends DatabaseAccessSupport {
  val logger = LoggerFactory.getLogger(getClass)

  implicit val getSocialAccountsHotelResult = GetResult(r => SocialAccountsHotel(r.<<, r.<<,
    r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

  def findById(profileId: Int, queryId: Int) = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[SocialAccountsHotel](
          s"""select m.fk_eng_engagement_data_quer_id, h.HOTEL_ID, h.TOTAL_RATING, h.HOTEL_NAME, h.HOTEL_ADDRESS,
              h.HOTEL_STARS, h.TOTAL_REVIEWS, h.HOTEL_URL
                from eng_hotels h, eng_queryid_hotelid m
                where h.hotel_id = m.fk_eng_var_hotel_id
                  and h.is_enabled = 1
                  and m.fk_eng_engagement_data_quer_id = $queryId
                  and m.fk_eng_engagement_data_quer_id in (
                    select q.id from eng_engagement_data_queries q where q.is_active = 1
                        and fk_cust_social_engagement_id in (
                           select s.id from eng_cust_social_credentials s where s.fk_cust_id = $profileId )
                    )
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
              s"""select m.fk_eng_engagement_data_quer_id, h.HOTEL_ID, h.TOTAL_RATING, h.HOTEL_NAME, h.HOTEL_ADDRESS,
              h.HOTEL_STARS, h.TOTAL_REVIEWS, h.HOTEL_URL
                from eng_hotels h, eng_queryid_hotelid m
                where h.hotel_id = m.fk_eng_var_hotel_id
                  and h.is_enabled = 1
                  and m.fk_eng_engagement_data_quer_id in (
                    select q.id from eng_engagement_data_queries q where q.is_active = 1
                        and fk_cust_social_engagement_id in (
                           select s.id from eng_cust_social_credentials s where s.fk_cust_id = $profileId )
                    )
                    order by h.TOTAL_RATING desc """)
            val accounts = records.list()
            SocialAccounts("hotel", accounts)

        })
    }
    prom.future
  }


  def addAccount(profileId: Int, cred: SocialCredentialsHotel) {
    getConnection withSession {
      var myId = 0

      val datasourceId = cred.hotelDatasource match {
        case "tripadvisor" => 3
        case "booking" => 13
        case "expedia" => 16
        case "zoover" => 17
        case "holydaycheck" => 18
      }
      implicit session =>
        try {

          //add account
          (Q.u + s"""insert into eng_cust_social_credentials (ID, FK_DATASOURCE_ID, FK_CUST_ID, HOTELS)
          values (SEQ_ENG_CUST_SOCIAL_CREDENTIAL.nextval, $datasourceId, $profileId, '${cred.hotelName}')""").execute()

          // get Id
          val result = Q.queryNA[Int](
            s"""select id from eng_cust_social_credentials
               where  FK_DATASOURCE_ID = $datasourceId and HOTELS = '${cred.hotelName}' and fk_cust_id = $profileId
              order by id desc """)
          myId = result.first()
          addAccountHotelQueries(myId, cred)
          logger.info("---------->  Id  $myId ")
          myId
        } catch {
          case e: Exception => {
            SocialAccountsQueriesDao.deleteSocialCredentialsExc(myId)
            logger.error("---------->  addAccount ganalytics " + e.printStackTrace())
          }
        }
    }
  }

  private def addAccountHotelQueries(credId: Int, cred: SocialCredentialsHotel) {
    try {
      getConnection withSession {
        implicit session =>
          val ffsl = "HOTELS_DATA"
          // add queries
          (Q.u +
            s""" insert into  eng_engagement_data_queries (ID, FK_CUST_SOCIAL_ENGAGEMENT_ID, IS_ACTIVE, Q_TTS, LAST_UPDATE, ATTR)
            values (SEQ_ENG_ENGAGEMENT_DATA_QUERIE.nextval, $credId, 1, 86400, sysdate-1, '$ffsl') """).execute()
          // get the queryId so we can use it
          val myQueryId = Q.queryNA[Int](
            s"""select id from eng_engagement_data_queries
               where  FK_CUST_SOCIAL_ENGAGEMENT_ID = $credId and ATTR = '$ffsl' order by id desc""").first()

          // check if the hotel url already exists in our database
          var hotelId = 0
          hotelId = Q.queryNA[Int]( s""" select hotel_id from eng_hotels where hotel_url = '${cred.hotelUrl}' """).first()

          if (hotelId == 0) {
            // we don't have this hotel url so we add it to our system
            (Q.u +
              s""" insert into  eng_hotels (HOTEL_ID, HOTEL_URL)
            values (ENG_HOTELS_SEQ.nextval, ${cred.hotelUrl}) """).execute()

            // get the new hotel id
            hotelId = Q.queryNA[Int]( s""" select hotel_id from eng_hotels where hotel_url = '${cred.hotelUrl}' """).first()

            // add query id with hotel id
            (Q.u +
              s""" insert into  ENG_QUERYID_HOTELID (FK_ENG_ENGAGEMENT_DATA_QUER_ID, FK_ENG_VAR_HOTEL_ID)
            values ($myQueryId, $hotelId) """).execute()

          } else {
            // add query id with hotel id
            (Q.u +
              s""" insert into  ENG_QUERYID_HOTELID (FK_ENG_ENGAGEMENT_DATA_QUER_ID, FK_ENG_VAR_HOTEL_ID)
            values ($myQueryId, $hotelId) """).execute()
          }
      }
    } catch {
      case e: Exception => logger.error("---------->  Error on inserting the hotel queries " + e.printStackTrace())
    }
  }

  def deleteHotel(profileId: Int, credId: Int) {
    try {
      getConnection withSession {
        implicit session =>
          // delete ENG_QUERYID_HOTELID query
          (Q.u + s""" delete from eng_queryid_hotelid m
                    where m.fk_eng_engagement_data_quer_id in (
                        select q.id from eng_engagement_data_queries q where fk_cust_social_engagement_id  = $credId
                          and fk_cust_social_engagement_id in (
                               select s.id from eng_cust_social_credentials s where s.fk_cust_id = $profileId )
                        )""").execute()

          // delete the eng queries
          (Q.u + s""" delete from eng_engagement_data_queries q where fk_cust_social_engagement_id = $credId
            and fk_cust_social_engagement_id in (
                               select s.id from eng_cust_social_credentials s where s.fk_cust_id = $profileId )
                        )""").execute()

          // delete the eng credentials
          (Q.u + s""" delete from eng_cust_social_credentials s where s.fk_cust_id = $profileId and s.id = $credId""").execute()

      }
    } catch {
      case e: Exception => logger.error("---------->  Error on deleting the hotel queries " + e.printStackTrace())
    }
  }

}


object SocialAccountsQueriesDao extends DatabaseAccessSupport {
  val logger = LoggerFactory.getLogger(getClass)

  def insertQueries(credId: Int, datasource: String) {
    try {
      getConnection withSession {
        implicit session =>

          datasource match {
            case "twitter" => {
              val ffsl = "TW_FFSL"
              val query1 = "TW_Retweets"
              val query2 = "TW_Mentions"
              // add queries
              (Q.u +
                s""" insert into  eng_engagement_data_queries (ID, FK_CUST_SOCIAL_ENGAGEMENT_ID, IS_ACTIVE, Q_TTS, LAST_UPDATE, ATTR)
            values (SEQ_ENG_ENGAGEMENT_DATA_QUERIE.nextval, $credId, 1, 86400, sysdate-1, '$ffsl') """).execute()
              (Q.u +
                s""" insert into  eng_engagement_data_queries (ID, FK_CUST_SOCIAL_ENGAGEMENT_ID, IS_ACTIVE, Q_TTS, LAST_UPDATE, ATTR)
            values (SEQ_ENG_ENGAGEMENT_DATA_QUERIE.nextval, $credId, 1, 2000, sysdate-1, '$query1') """).execute()
              (Q.u +
                s""" insert into  eng_engagement_data_queries (ID, FK_CUST_SOCIAL_ENGAGEMENT_ID, IS_ACTIVE, Q_TTS, LAST_UPDATE, ATTR)
            values (SEQ_ENG_ENGAGEMENT_DATA_QUERIE.nextval, $credId, 1, 2000, sysdate-1, '$query2') """).execute()
            }
            case "facebook" => {
              val ffsl = "FB_FFSL"
              val query1 = "FB_FANPAGE_WALL"
              // add queries
              (Q.u +
                s""" insert into  eng_engagement_data_queries (ID, FK_CUST_SOCIAL_ENGAGEMENT_ID, IS_ACTIVE, Q_TTS, LAST_UPDATE, ATTR)
            values (SEQ_ENG_ENGAGEMENT_DATA_QUERIE.nextval, $credId, 1, 86400, sysdate-1, '$ffsl') """).execute()
              (Q.u +
                s""" insert into  eng_engagement_data_queries (ID, FK_CUST_SOCIAL_ENGAGEMENT_ID, IS_ACTIVE, Q_TTS, LAST_UPDATE, ATTR)
            values (SEQ_ENG_ENGAGEMENT_DATA_QUERIE.nextval, $credId, 1, 2000, sysdate-1, '$query1') """).execute()
            }
            case "youtube" => {
              val ffsl = "YT_FFSL"
              val query1 = "YT_USER_WALL"
              // add queries
              (Q.u +
                s""" insert into  eng_engagement_data_queries (ID, FK_CUST_SOCIAL_ENGAGEMENT_ID, IS_ACTIVE, Q_TTS, LAST_UPDATE, ATTR)
            values (SEQ_ENG_ENGAGEMENT_DATA_QUERIE.nextval, $credId, 1, 86400, sysdate-1, '$ffsl') """).execute()
              (Q.u +
                s""" insert into  eng_engagement_data_queries (ID, FK_CUST_SOCIAL_ENGAGEMENT_ID, IS_ACTIVE, Q_TTS, LAST_UPDATE, ATTR)
            values (SEQ_ENG_ENGAGEMENT_DATA_QUERIE.nextval, $credId, 1, 2000, sysdate-1, '$query1') """).execute()
            }
            case "ganalytics" => {
              val ffsl = "GA_STATS"
              // add queries
              (Q.u +
                s""" insert into  eng_engagement_data_queries (ID, FK_CUST_SOCIAL_ENGAGEMENT_ID, IS_ACTIVE, Q_TTS, LAST_UPDATE, ATTR)
            values (SEQ_ENG_ENGAGEMENT_DATA_QUERIE.nextval, $credId, 1, 86400, sysdate-1, '$ffsl') """).execute()
            }

          }
      }
    } catch {
      case e: Exception => logger.error("---------->  Not able to insert the queries " + e.printStackTrace())
    }
  }

  def deleteSocialCredentials(profileId: Int, credId: Int) {
    getConnection withSession {
      implicit session =>
        try {
          (Q.u + s""" delete from eng_engagement_data_queries q where FK_CUST_SOCIAL_ENGAGEMENT_ID = $credId
                        and fk_cust_social_engagement_id in (
                                   select s.id from eng_cust_social_credentials s where s.fk_cust_id = $profileId )""").execute()

          (Q.u + s""" delete from eng_cust_social_credentials s where s.fk_cust_id = $profileId and s.id = $credId""").execute()
        } catch {
          case e: Exception => logger.error("---------->  deleteSocialCredentials " + e.printStackTrace())
        }
    }
  }

  def deleteSocialCredentialsExc(credId: Int) {
    getConnection withSession {
      implicit session =>
        try {
          (Q.u + s""" delete from eng_engagement_data_queries q where FK_CUST_SOCIAL_ENGAGEMENT_ID = $credId""").execute()

          (Q.u + s""" delete from eng_cust_social_credentials s where s.id = $credId""").execute()
        } catch {
          case e: Exception => logger.error("---------->  deleteSocialCredentials " + e.printStackTrace())
        }
    }
  }

}