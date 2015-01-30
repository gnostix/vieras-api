package gr.gnostix.api.models.publicSearch

import gr.gnostix.api.db.plainsql.DatabaseAccessSupportOra
import gr.gnostix.api.models.plainModels._
import gr.gnostix.api.utilities.SqlUtils
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory

import scala.slick.jdbc.{GetResult, StaticQuery => Q}


object DatafindingsThirdLevelDataDAO extends DatabaseAccessSupportOra {

  implicit val getThirdLevelDataTwitterResult = GetResult(r => ThirdLevelDataTwitter(FirstLevelData(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)
    , SecondLevelDataTwitter(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)))
  implicit val getThirdLevelDataFacebookResult = GetResult(r => ThirdLevelDataFacebook(FirstLevelData(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<),
    SecondLevelDataFacebook(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)))
  implicit val getThirdLevelDataGplusResult = GetResult(r => ThirdLevelDataGplus(FirstLevelData(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<),
    SecondLevelDataGplus(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)))
  implicit val getThirdLevelDataYoutubeResult = GetResult(r => ThirdLevelDataYoutube(FirstLevelData(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<),
    SecondLevelDataYoutube(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)))
  implicit val getThirdLevelDataWebResult = GetResult(r => ThirdLevelDataWeb(FirstLevelData(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<),
    SecondLevelDataWeb(r.<<, r.<<)))
  implicit val getThirdLevelDataFeed = GetResult(r => ThirdLevelDataFeed(FirstLevelData(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<),
    SecondLevelDataFeed(r.<<)))


  val logger = LoggerFactory.getLogger(getClass)


  def getThirdLevelDataDefault(fromDate: DateTime, toDate: DateTime, profileId: Int, datasource: String): SocialData = {
    val mySqlDynamic = SqlUtils.getDataDefaultObj(profileId)
    //bring the actual data
    getThirdLevelData(fromDate, toDate, mySqlDynamic, datasource)
  }

  def getThirdLevelDataByKeywords(fromDate: DateTime, toDate: DateTime, profileId: Int, keywords: List[Int], datasource: String): SocialData = {
    val mySqlDynamic = SqlUtils.getDataByKeywordsObj(profileId, keywords)
    //bring the actual data
    getThirdLevelData(fromDate, toDate, mySqlDynamic, datasource)
  }

  def getThirdLevelDataByTopics(fromDate: DateTime, toDate: DateTime, profileId: Int, topics: List[Int], datasource: String): SocialData = {
    val mySqlDynamic = SqlUtils.getDataByTopicsObj(profileId, topics)
    //bring the actual data
    getThirdLevelData(fromDate, toDate, mySqlDynamic, datasource)
  }


  def getThirdLevelData(fromDate: DateTime, toDate: DateTime, mySqlDynamic: String, datasource: String) = {

    val sqlQ = buildQuery(fromDate, toDate, mySqlDynamic, datasource)
    var myData = datasource match {
      case "twitter" => List[ThirdLevelDataTwitter]()
      case "facebook" => List[ThirdLevelDataFacebook]()
      case "youtube" => List[ThirdLevelDataYoutube]()
      case "gplus" => List[ThirdLevelDataGplus]()
      case "web" => List[ThirdLevelDataWeb]()
      case "linkedin" => List[ThirdLevelDataWeb]()
      case "blog" => List[ThirdLevelDataFeed]()
      case "news" => List[ThirdLevelDataFeed]()
      case "personal" => List[ThirdLevelDataFeed]()
    }

    getConnection withSession {
      implicit session =>
        logger.info("getThirdLevelData tw ------------->" + sqlQ)
        val records = datasource match {
          case "twitter" => Q.queryNA[ThirdLevelDataTwitter](sqlQ)
          case "facebook" => Q.queryNA[ThirdLevelDataFacebook](sqlQ)
          case "gplus" => Q.queryNA[ThirdLevelDataGplus](sqlQ)
          case "youtube" => Q.queryNA[ThirdLevelDataYoutube](sqlQ)
          case "web" => Q.queryNA[ThirdLevelDataWeb](sqlQ)
          case "linkedin" => Q.queryNA[ThirdLevelDataWeb](sqlQ)
          case "blog" => Q.queryNA[ThirdLevelDataFeed](sqlQ)
          case "news" => Q.queryNA[ThirdLevelDataFeed](sqlQ)
          case "personal" => Q.queryNA[ThirdLevelDataFeed](sqlQ)

        }
        myData = records.list()
    }


    val lineData = datasource match {
      case "twitter" => SocialData(SocialDatasources.twitter, myData)
      case "facebook" => SocialData(SocialDatasources.facebook, myData)
      case "gplus" => SocialData(SocialDatasources.gplus, myData)
      case "youtube" => SocialData(SocialDatasources.youtube, myData)
      case "web" => SocialData(SocialDatasources.web, myData)
      case "linkedin" => SocialData(SocialDatasources.linkedin, myData)
      case "blog" => SocialData(SocialDatasources.blog, myData)
      case "news" => SocialData(SocialDatasources.news, myData)
      case "personal" => SocialData(SocialDatasources.personal, myData)
    }

    lineData
  }


  def buildQuery(fromDate: DateTime, toDate: DateTime, sqlDynamic: String, datasource: String): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"


    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sql = datasource match {
      case "twitter" => getSqlTW(fromDateStr, toDateStr, sqlDynamic)
      case "facebook" => getSqlFB(fromDateStr, toDateStr, sqlDynamic)
      case "gplus" => getSqlGplus(fromDateStr, toDateStr, sqlDynamic)
      case "youtube" => getSqlYT(fromDateStr, toDateStr, sqlDynamic)
      case "web" => getSqlWebByType(fromDateStr, toDateStr, sqlDynamic, WebDatasources.web.head._1)
      case "linkedin" => getSqlWebByType(fromDateStr, toDateStr, sqlDynamic, WebDatasources.linkedin.head._1)
      case "news" => getSqlFeedByType(fromDateStr, toDateStr, sqlDynamic, FeedDatasources.news.head._1)
      case "blog" => getSqlFeedByType(fromDateStr, toDateStr, sqlDynamic, FeedDatasources.blogs.head._1)
      case "personal" => getSqlFeedByType(fromDateStr, toDateStr, sqlDynamic, FeedDatasources.personal.head._1)
      case _ => {
        logger.info("---------> The Third Level Data  ")
        s"no sql code for this datasource ${datasource}"
      }
    }
    sql
  }

  def getSqlTW(fromDateStr: String, toDateStr: String, sqlGetProfileData: String): String = {
    val sql = s"""select t.tw_id, t.t_text, t.t_from_user, t_created_at, t.fk_query_id, M.SENTIMENT as sentiment,
                    'www.twitter.com/' || t_from_user || '/statuses/' || t_tweet_id  as msg_Url,
                    T_PROFILE_IMAGE_URL, T_FROM_USER_ID, T_TO_USER, T_TWEET_ID, FOLLOWERS,
                          FOLLOWING, TWEETS_NUM, USER_URL, LISTED
                      from twitter_results t,MSG_ANALYTICS m
                      where TW_ID = M.FK_MSG_ID and show_flag !=0 and fk_query_id in (select q_id from queries where  ${sqlGetProfileData} )
                        and t_created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                  order by t_created_at asc"""
    sql
    //logger.info("------------>" + sql)
  }


  def getSqlFB(fromDateStr: String, toDateStr: String, sqlGetProfileData: String): String = {
    val sql = s"""select fb_id, SUBSTR(NVL(f_message, NVL(f_link_caption, NVL(f_link_description,''))),1,100), f_from_name as from_user, f_created_time,
                  fk_query_id,  M.SENTIMENT as sentiment, 'www.facebook.com/' || f_from_id || '/posts/'|| f_msg_id as msg_Url,
                  f_from_id, f_comments, shares, f_likes, f_icon_link, f_link, f_picture
                      from facebook_results
                      inner join  MSG_ANALYTICS m on FB_ID = M.FK_MSG_ID
                      where show_flag !=0 and fk_query_id in (select q_id from queries where  ${sqlGetProfileData} )
                        and f_created_time between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    order by f_created_time asc"""

    sql

  }

  def getSqlGplus(fromDateStr: String, toDateStr: String, sqlGetProfileData: String): String = {
    val sql = s"""select gplus_id, SUBSTR(NVL(itemtitle, NVL(itemcontent, NVL(attachname,''))),1,100), actorname, itemdate, fk_query_id,
                   M.SENTIMENT as sentiment, ITEMURL, ITEMID, PLUSONERS, RESHARERS, REPLIES, ACTORID, ACTORURL, ACTORIMAGE, ATTACHURL
                      from googleplus_results i
                      inner join  MSG_ANALYTICS m on GPLUS_ID = M.FK_MSG_ID
                      where itemdate between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                       and fk_query_id in (select q_id from queries where  ${sqlGetProfileData} )
                       and show_flag !=0
                      ORDER  BY itemdate asc"""

    sql

  }

  def getSqlYT(fromDateStr: String, toDateStr: String, sqlGetProfileData: String): String = {
    val sql = s"""select YOU_ID, SUBSTR(NVL(y_title, NVL(description, '')),1,100), Y_AUTHOR_NAME, Y_PUBLISHED_AT, fk_query_id,
              M.SENTIMENT as sentiment, y_player_url,y_video_id, y_favorite_count, y_view_count, y_dislike_count, y_like_count, channel_id
                      from youtube_results i
                      inner join  MSG_ANALYTICS m on YOU_ID = M.FK_MSG_ID
                      where Y_PUBLISHED_AT between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                       and fk_query_id in (select q_id from queries where  ${sqlGetProfileData} )
                       and show_flag !=0
                      ORDER  BY Y_PUBLISHED_AT"""

    sql

  }

  def getSqlWebByType(fromDateStr: String, toDateStr: String, sqlGetProfileData: String, webType: Int): String = {
    val sql = s"""select web_id, SUBSTR(title,1,100), null as author,item_date, fk_queries_id, M.SENTIMENT as sentiment, url,url, description
                    from web_Results i
                    inner join  MSG_ANALYTICS m on web_id = M.FK_MSG_ID
                    where item_date between TO_DATE('07-07-2012 00:00:00', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('09-07-2014 23:59:59', 'DD-MM-YYYY HH24:MI:SS')
                    and fk_grp_id = ${webType}
                    and fk_queries_id in (select q_id from queries where  ${sqlGetProfileData} )
                    and show_flag !=0
                  order  BY item_date"""

     sql

  }


  def getSqlFeedByType(fromDateStr: String, toDateStr: String, sqlGetProfileData: String, feedType: Int): String = {
    val sql = s"""select feed_id, SUBSTR(title,1,100), null as author, RSS_DATE, FK_QUERIES_ID , M.SENTIMENT as sentiment,
              LINK_GUID, SUBSTR(message,1,1000) || '..'
                    from feed_results i
                    inner join  MSG_ANALYTICS m on feed_id = M.FK_MSG_ID
                      where RSS_DATE between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                      and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')   and fk_grp_id  = ${feedType}
                      and fk_queries_id in (select q_id from queries where ${sqlGetProfileData} )
                      and show_flag !=0
                  order by RSS_DATE asc"""

    sql

  }


}
