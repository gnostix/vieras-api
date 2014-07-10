package gr.gnostix.api.models

import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import gr.gnostix.api.utilities.{DateUtils, SqlUtils}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory
import scala.slick.jdbc.{StaticQuery => Q, GetResult}


object DatafindingsFirstLevelDataDAO extends DatabaseAccessSupport {

  implicit val getFirstleveldataResult = GetResult(r => FirstLevelData(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)


  def getFirstLevelDataDefault(fromDate: DateTime, toDate: DateTime, profileId: Int, datasource: String): SocialData = {
    val mySqlDynamic = SqlUtils.getDataDefaultObj(profileId)
    //bring the actual data
    getFirstLevelData(fromDate, toDate, profileId, mySqlDynamic, datasource)
  }

  def getFirstLevelDataByKeywords(fromDate: DateTime, toDate: DateTime, profileId: Int, keywords: List[Int], datasource: String): SocialData = {
    val mySqlDynamic = SqlUtils.getDataByKeywordsObj(profileId, keywords)
    //bring the actual data
    getFirstLevelData(fromDate, toDate, profileId, mySqlDynamic, datasource)
  }

  def getFirstLevelDataByTopics(fromDate: DateTime, toDate: DateTime, profileId: Int, topics: List[Int], datasource: String): SocialData = {
    val mySqlDynamic = SqlUtils.getDataByTopicsObj(profileId, topics)
    //bring the actual data
    getFirstLevelData(fromDate, toDate, profileId, mySqlDynamic, datasource)
  }


  def getFirstLevelData(fromDate: DateTime, toDate: DateTime, profileId: Int, sqlDynamicKeywordsTopics: String, datasource: String) = {

    val sqlQ = buildQuery(fromDate, toDate, profileId, sqlDynamicKeywordsTopics, datasource)
    var myData = List[FirstLevelData]()

    getConnection withSession {
      implicit session =>
        logger.info("getFirstLevelData ------------->" + sqlQ)
        val records = Q.queryNA[FirstLevelData](sqlQ)
        myData = records.list()
    }
    val lineData = SocialData(datasource, myData)
    lineData
  }


  def buildQuery(fromDate: DateTime, toDate: DateTime, profileId: Int, sqlDynamicKeywordsTopics: String, datasource: String): String = {
    logger.info("-------------> buildTwQuery -----------")

    /*    val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
        logger.info("------------->" + numDays + "-----------")*/

    val datePattern = "dd-MM-yyyy HH:mm:ss"


    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    logger.info(s"--------->  datasource: ${datasource}")

    val sql = datasource match {
      case "twitter" => getSqlTW(fromDateStr, toDateStr, sqlDynamicKeywordsTopics)
      case "facebook" => getSqlFB(fromDateStr, toDateStr, sqlDynamicKeywordsTopics)
      case "gplus" => getSqlGplus(fromDateStr, toDateStr, sqlDynamicKeywordsTopics)
      case "youtube" => getSqlYT(fromDateStr, toDateStr, sqlDynamicKeywordsTopics)
      case "web" => getSqlWebByType(fromDateStr, toDateStr, sqlDynamicKeywordsTopics, WebDatasources.web.head._1)
      case "linkedin" => getSqlWebByType(fromDateStr, toDateStr, sqlDynamicKeywordsTopics, WebDatasources.linkedin.head._1)
      case "news" => getSqlFeedByType(fromDateStr, toDateStr, sqlDynamicKeywordsTopics, FeedDatasources.news.head._1)
      case "blog" => getSqlFeedByType(fromDateStr, toDateStr, sqlDynamicKeywordsTopics, FeedDatasources.blogs.head._1)
      case "personal" => getSqlFeedByType(fromDateStr, toDateStr, sqlDynamicKeywordsTopics, FeedDatasources.personal.head._1)
      case _ => {
        logger.info("---------> For the First Level Data we don't match a datasource.. !")
        s"no sql code for this datasource ${datasource}"
      }
    }
    sql
  }

  def getSqlTW(fromDateStr: String, toDateStr: String, sqlGetProfileData: String): String = {
    val sql = s"""select t.t_tweet_id, t.t_text, t.t_from_user, t_created_at, t.fk_query_id, M.SENTIMENT as sentiment,
                    'www.twitter.com/' || t_from_user || '/statuses/' || t_tweet_id  as msg_Url
                      from twitter_results t,MSG_ANALYTICS m
                      where TW_ID = M.FK_MSG_ID and show_flag !=0 and fk_query_id in (select q_id from queries where  ${sqlGetProfileData} )
                        and t_created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                  order by t_created_at asc"""
    sql
    //logger.info("------------>" + sql)
  }

  def getSqlFB(fromDateStr: String, toDateStr: String, sqlGetProfileData: String): String = {
    val sql = s"""select f_msg_id, SUBSTR(NVL(f_message, NVL(f_link_caption, NVL(f_link_description,''))),1,100), f_from_name as from_user, f_created_time,
                  fk_query_id,  M.SENTIMENT as sentiment, 'www.facebook.com/' || f_from_id || '/posts/'|| f_msg_id as msg_Url
                      from facebook_results
                      inner join  MSG_ANALYTICS m on FB_ID = M.FK_MSG_ID
                      where show_flag !=0 and fk_query_id in (select q_id from queries where  ${sqlGetProfileData} )
                        and f_created_time between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    order by f_created_time asc"""
    sql

    //logger.info("------------>" + sql)
  }

  def getSqlGplus(fromDateStr: String, toDateStr: String, sqlGetProfileData: String): String = {
    val sql = s"""select itemid, SUBSTR(NVL(itemtitle, NVL(itemcontent, NVL(attachname,''))),1,100), actorname, itemdate, fk_query_id,  M.SENTIMENT as sentiment, ITEMURL
                      from googleplus_results i
                      inner join  MSG_ANALYTICS m on GPLUS_ID = M.FK_MSG_ID
                      where itemdate between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                       and fk_query_id in (select q_id from queries where  ${sqlGetProfileData} )
                       and show_flag !=0
                      ORDER  BY itemdate asc"""
    sql

    //logger.info("------------>" + sql)
  }

  def getSqlYT(fromDateStr: String, toDateStr: String, sqlGetProfileData: String): String = {
    val sql = s"""select Y_VIDEO_ID, SUBSTR(NVL(y_title, NVL(description, '')),1,100), Y_AUTHOR_NAME, Y_PUBLISHED_AT, fk_query_id,  M.SENTIMENT as sentiment, y_player_url
                      from youtube_results i
                      inner join  MSG_ANALYTICS m on YOU_ID = M.FK_MSG_ID
                      where Y_PUBLISHED_AT between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                       and fk_query_id in (select q_id from queries where  ${sqlGetProfileData} )
                       and show_flag !=0
                      ORDER  BY Y_PUBLISHED_AT"""
    sql

    //logger.info("------------>" + sql)
  }

  def getSqlWebByType(fromDateStr: String, toDateStr: String, sqlGetProfileData: String, webType: Int): String = {
    val sql = s"""select web_id, SUBSTR(title,1,100), null as author,item_date, fk_queries_id, M.SENTIMENT as sentiment, url
                    from web_Results i
                    inner join  MSG_ANALYTICS m on web_id = M.FK_MSG_ID
                    where item_date between TO_DATE('07-07-2012 00:00:00', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('09-07-2014 23:59:59', 'DD-MM-YYYY HH24:MI:SS')
                    and fk_grp_id = ${webType}
                    and fk_query_id in (select q_id from queries where  ${sqlGetProfileData} )
                    and show_flag !=0
                  order  BY item_date"""
    sql

    //logger.info("------------>" + sql)
  }



  def getSqlFeedByType(fromDateStr: String, toDateStr: String, sqlGetProfileData: String, feedType: Int): String = {
    val sql = s"""select feed_id, SUBSTR(title,1,100), null as author, RSS_DATE, FK_QUERIES_ID , M.SENTIMENT as sentiment, LINK_GUID
                    from feed_results i
                    inner join  MSG_ANALYTICS m on feed_id = M.FK_MSG_ID
                      where RSS_DATE between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                      and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')   and fk_grp_id  = ${feedType}
                      and fk_queries_id in (select q_id from queries where ${sqlGetProfileData} )
                      and show_flag !=0
                  order by RSS_DATE asc"""
    sql

    //logger.info("------------>" + sql)
  }


}
