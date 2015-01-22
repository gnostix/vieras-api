package gr.gnostix.api.models.publicSearch

import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import gr.gnostix.api.models.plainModels.{SocialData, DataLineGraph}
import gr.gnostix.api.utilities.{DateUtils, SqlUtils}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory

import scala.slick.jdbc.{GetResult, StaticQuery => Q}

object FeedDatasources {
  // this is the id from datasources for news portals and the same for the next feed categories
  val news = Map(5 -> "news")
  val blogs = Map(3 -> "blogs")
  val personal = Map(13 -> "personal")
}

object DtFeedLineGraphDAO extends DatabaseAccessSupport {

  implicit val getDtFeedLineGraphResult = GetResult(r => DataLineGraph(r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)

  def getLineDataDefault(fromDate: DateTime, toDate: DateTime, profileId: Int, feedType: Map[Int, String]): SocialData = {
    val mySqlDynamic = SqlUtils.getDataDefaultObj(profileId)
    //bring the actual data
    getLineData(fromDate, toDate, profileId, feedType, mySqlDynamic)
  }

  def getLineDataByKeywords(fromDate: DateTime, toDate: DateTime, profileId: Int, keywords: List[Int], feedType: Map[Int, String]): SocialData = {
    val mySqlDynamic = SqlUtils.getDataByKeywordsObj(profileId, keywords)
    //bring the actual data
    getLineData(fromDate, toDate, profileId, feedType, mySqlDynamic)
  }

  def getLineDataByTopics(fromDate: DateTime, toDate: DateTime, profileId: Int, topics: List[Int], feedType: Map[Int, String]): SocialData = {
    val mySqlDynamic = SqlUtils.getDataByTopicsObj(profileId, topics)
    //bring the actual data
    getLineData(fromDate, toDate, profileId, feedType, mySqlDynamic)
  }

  def getLineData(fromDate: DateTime, toDate: DateTime, profileId: Int, feedType: Map[Int, String], sqlDynamicKeywordsTopics: String): SocialData = {

    val sqlQ = buildQuery(fromDate, toDate, profileId, feedType.head._1, sqlDynamicKeywordsTopics)
    var myData = List[DataLineGraph]()

    getConnection withSession {
      implicit session =>
        logger.info("getLineData ------------->" + sqlQ)
        val records = Q.queryNA[DataLineGraph](sqlQ)
        myData = records.list()
    }

    val lineData = feedType.head._2 match {
      case "blogs" => SocialData("blogs", myData)
      case "news" => SocialData("news", myData)
      case "personal" => SocialData("personal", myData)
    }
    lineData

  }


  def buildQuery(fromDate: DateTime, toDate: DateTime, profileId: Int, feedType: Int, sqlDynamicKeywordsTopics: String): String = {
    logger.info("-------------> buildQuery -----------")

    val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
    logger.info("------------->" + numDays + "-----------")

    val datePattern = "dd-MM-yyyy HH:mm:ss"


    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)
    getSql(numDays, fromDateStr, toDateStr, profileId, feedType, sqlDynamicKeywordsTopics)
  }


  def getSql(numDays: Int, fromDateStr: String, toDateStr: String, profileId: Int, feedType: Int, sqlGetProfileData: String) = {
    if (numDays == 0) {
      val sql = s"""select count(*), trunc(RSS_DATE,'HH') from feed_results i
                           where RSS_DATE between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')   and fk_grp_id  = ${feedType} and
                           fk_queries_id in (select q_id from queries where ${sqlGetProfileData} )
                           group  BY trunc(RSS_DATE,'HH')
                           order by trunc(RSS_DATE, 'HH') asc"""
      logger.info("------------>" + sql)
      sql
    } else if (numDays >= 1 && numDays <= 30) {
      val sql = s"""select count(*), trunc(RSS_DATE) from feed_results i
                           where RSS_DATE between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')   and fk_grp_id  = ${feedType} and
                           fk_queries_id in (select q_id from queries where ${sqlGetProfileData} )
                           group  BY trunc(RSS_DATE)
                           order by trunc(RSS_DATE) asc"""
      sql
    } else if (numDays > 30 && numDays < 90) {
      val sql = s"""select count(*), trunc(RSS_DATE,'ww') from feed_results i
                           where RSS_DATE between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')   and fk_grp_id  = ${feedType} and
                           fk_queries_id in (select q_id from queries where ${sqlGetProfileData} )
                           group  BY trunc(RSS_DATE,'ww')
                           order by trunc(RSS_DATE, 'ww') asc"""
      sql
    } else {
      val sql = s"""select count(*), trunc(RSS_DATE,'month') from feed_results i
                           where RSS_DATE between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')   and fk_grp_id  = ${feedType} and
                           fk_queries_id in (select q_id from queries where ${sqlGetProfileData} )
                           group  BY trunc(RSS_DATE,'month')
                           order by trunc(RSS_DATE, 'month') asc"""
      sql
    }
  }


}