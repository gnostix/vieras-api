package gr.gnostix.api.models

import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
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
}
