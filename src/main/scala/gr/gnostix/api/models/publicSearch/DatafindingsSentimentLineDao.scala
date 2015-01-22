package gr.gnostix.api.models.publicSearch

import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import gr.gnostix.api.models.plainModels.{SentimentLine, SocialData}
import gr.gnostix.api.utilities.SqlUtils
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory

import scala.slick.jdbc.{GetResult, StaticQuery => Q}

object DatafindingsSentimentLineDao extends DatabaseAccessSupport {

  implicit val getSentimentLineResult = GetResult(r => SentimentLine(r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)


  def getDataDefault(fromDate: DateTime, toDate: DateTime, profileId: Int, datasource: String): Option[SocialData] = {
    val mySqlDynamic = SqlUtils.getDataDefaultObj(profileId)
    //bring the actual data
    val data = getData(fromDate, toDate, mySqlDynamic, datasource)
    data match {
      case Some(data) => Some(data)
      case None => None
    }
  }

  def getDataByKeywords(fromDate: DateTime, toDate: DateTime, profileId: Int, keywords: List[Int], datasource: String): Option[SocialData] = {
    val mySqlDynamic = SqlUtils.getDataByKeywordsObj(profileId, keywords)
    //bring the actual data
    val data = getData(fromDate, toDate, mySqlDynamic, datasource)
    data match {
      case Some(data) => Some(data)
      case None => None
    }
  }

  def getDataByTopics(fromDate: DateTime, toDate: DateTime, profileId: Int, topics: List[Int], datasource: String): Option[SocialData] = {
    val mySqlDynamic = SqlUtils.getDataByTopicsObj(profileId, topics)
    //bring the actual data
    val data = getData(fromDate, toDate, mySqlDynamic, datasource)
    data match {
      case Some(data) => Some(data)
      case None => None
    }
  }



  private def getData(fromDate: DateTime, toDate: DateTime, mySqlDynamic: String, datasource: String):Option[SocialData]  = {

    val sql = buildQuery(fromDate, toDate, mySqlDynamic, datasource)

    sql match {
      case Some(sql) => {

        var myData = List[SentimentLine]()

        getConnection withSession {
          implicit session =>
            logger.info("getThirdLevelData tw ------------->" + sql)
            val records = Q.queryNA[SentimentLine](sql)
            myData = records.list()
        }
        val lineData = SocialData(datasource, myData)
        Some(lineData)
      }

      case None => None
    }

  }

  private def buildQuery(fromDate: DateTime, toDate: DateTime, sqlDynamic: String, datasource: String): Option[String] = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"


    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    datasource match {
      case "twitter" => Some( getSqlTW(fromDateStr, toDateStr, sqlDynamic) )
      case "facebook" => Some( getSqlFB(fromDateStr, toDateStr, sqlDynamic) )
      case "gplus" => Some( getSqlGplus(fromDateStr, toDateStr, sqlDynamic) )
      case "youtube" => Some( getSqlYT(fromDateStr, toDateStr, sqlDynamic) )
      case "web" => Some( getSqlWebByType(fromDateStr, toDateStr, sqlDynamic, WebDatasources.web.head._1) )
      case "linkedin" => Some( getSqlWebByType(fromDateStr, toDateStr, sqlDynamic, WebDatasources.linkedin.head._1) )
      case "news" => Some( getSqlFeedByType(fromDateStr, toDateStr, sqlDynamic, FeedDatasources.news.head._1) )
      case "blog" => Some( getSqlFeedByType(fromDateStr, toDateStr, sqlDynamic, FeedDatasources.blogs.head._1) )
      case "personal" => Some( getSqlFeedByType(fromDateStr, toDateStr, sqlDynamic, FeedDatasources.personal.head._1) )
      case _ => {
        logger.info("---------> no sql code for this datasource ${datasource}  ")
        None
        }
    }

   }

  private def getSqlTW(fromDateStr: String, toDateStr: String, sqlGetProfileData: String): String = {
    val sql = s"""select count(*), m.sentiment from twitter_results i, msg_analytics m
                           where i.tw_id = m.fk_msg_id
                           and i.t_created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and i.show_flag != 0
                           and fk_query_id in (select q_id from queries where  ${sqlGetProfileData} )
                           group  BY m.sentiment
                           order by m.sentiment asc"""
    sql
    //logger.info("------------>" + sql)
  }


  private def getSqlFB(fromDateStr: String, toDateStr: String, sqlGetProfileData: String): String = {
    val sql = s"""select count(*), m.sentiment from facebook_results i, msg_analytics m
                           where i.fb_id = m.fk_msg_id
                           and i.f_created_time between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and i.show_flag != 0
                           and fk_query_id in (select q_id from queries where  ${sqlGetProfileData} )
                           group  BY m.sentiment
                           order by m.sentiment asc"""

    sql

  }

  private def getSqlGplus(fromDateStr: String, toDateStr: String, sqlGetProfileData: String): String = {
    val sql = s"""select count(*), m.sentiment from googleplus_results i, msg_analytics m
                           where i.gplus_id = m.fk_msg_id
                           and i.itemdate between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and i.show_flag != 0
                           and fk_query_id in (select q_id from queries where  ${sqlGetProfileData} )
                           group  BY m.sentiment
                           order by m.sentiment asc"""

    sql

  }

  private def getSqlYT(fromDateStr: String, toDateStr: String, sqlGetProfileData: String): String = {
    val sql = s"""select count(*), m.sentiment from youtube_results i, msg_analytics m
                           where i.YOU_ID = m.fk_msg_id
                           and i.Y_PUBLISHED_AT between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and i.show_flag != 0
                           and fk_query_id in (select q_id from queries where  ${sqlGetProfileData} )
                           group  BY m.sentiment
                           order by m.sentiment asc"""

    sql

  }

  private def getSqlWebByType(fromDateStr: String, toDateStr: String, sqlGetProfileData: String, webType: Int): String = {
    val sql = s"""select count(*), m.sentiment from web_Results i, msg_analytics m
                           where i.web_id = m.fk_msg_id
                           and i.item_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and i.show_flag != 0
                           and fk_grp_id = ${webType}
                           and fk_queries_id in (select q_id from queries where  ${sqlGetProfileData} )
                           group  BY m.sentiment
                           order by m.sentiment asc"""

    sql

  }


  private def getSqlFeedByType(fromDateStr: String, toDateStr: String, sqlGetProfileData: String, feedType: Int): String = {
    val sql = s"""select count(*), m.sentiment from feed_results i, msg_analytics m
                           where i.feed_id = m.fk_msg_id
                           and i.RSS_DATE between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and i.show_flag != 0
                           and fk_grp_id = ${feedType}
                           and fk_queries_id in (select q_id from queries where  ${sqlGetProfileData} )
                           group  BY m.sentiment
                           order by m.sentiment asc"""

    sql

  }


}
