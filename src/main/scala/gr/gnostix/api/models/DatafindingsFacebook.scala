package gr.gnostix.api.models

import java.sql.Timestamp
import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import scala.slick.jdbc.{GetResult, StaticQuery => Q}
import org.joda.time.{Days, DateTime}
import org.slf4j.LoggerFactory
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import gr.gnostix.api.utilities.{SqlUtils, DateUtils}


object DtFacebookLineGraphDAO extends DatabaseAccessSupport {

  implicit val getDtFacebookLineGraphResult = GetResult(r => DataLineGraph(r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)

  def getLineDataDefault(fromDate: DateTime, toDate: DateTime, profileId: Int): SocialData = {
    val mySqlDynamic = SqlUtils.getDataDefaultObj(profileId)
    //bring the actual data
    getLineData(fromDate, toDate, profileId, mySqlDynamic)
  }

  def getLineDataByKeywords(fromDate: DateTime, toDate: DateTime, profileId: Int, keywords: List[Int]): SocialData = {
    val mySqlDynamic = SqlUtils.getDataByKeywordsObj(profileId, keywords)
    //bring the actual data
    getLineData(fromDate, toDate, profileId, mySqlDynamic)
  }

  def getLineDataByTopics(fromDate: DateTime, toDate: DateTime, profileId: Int, topics: List[Int]): SocialData = {
    val mySqlDynamic = SqlUtils.getDataByTopicsObj(profileId, topics)
    //bring the actual data
    getLineData(fromDate, toDate, profileId, mySqlDynamic)
  }

  def getLineData(fromDate: DateTime, toDate: DateTime, profileId: Int, sqlDynamicKeywordsTopics: String) = {

    val sqlQ = buildQuery(fromDate, toDate, profileId, sqlDynamicKeywordsTopics)
    var myData = List[DataLineGraph]()

    getConnection withSession {
      implicit session =>
        logger.info("getLineData ------------->" + sqlQ)
        val records = Q.queryNA[DataLineGraph](sqlQ)
        myData = records.list()
    }
    val lineData = SocialData("facebook", myData)
    lineData
  }


  def buildQuery(fromDate: DateTime, toDate: DateTime, profileId: Int, sqlDynamicKeywordsTopics: String): String = {
    logger.info("-------------> buildQuery -----------")

    val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
    logger.info("------------->" + numDays + "-----------")

    val datePattern = "dd-MM-yyyy HH:mm:ss"

    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    getSql(numDays, fromDateStr, toDateStr, sqlDynamicKeywordsTopics)
  }

  def getSql(numDays: Int, fromDateStr: String, toDateStr: String, sqlGetProfileData: String) = {
    if (numDays == 0) {
      val sql = s"""select count(*), trunc(f_created_time,'HH') from facebook_results i
                           where f_created_time between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS') and
                           fk_query_id in (select q_id from queries where  ${sqlGetProfileData} )
                           group  BY trunc(f_created_time,'HH')
                           order by trunc(f_created_time, 'HH') asc"""
      logger.info("------------>" + sql)
      sql
    } else if (numDays >= 1 && numDays <= 30) {
      val sql = s"""select count(*), trunc(f_created_time) from facebook_results i
                           where f_created_time between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS') and
                           fk_query_id in (select q_id from queries where   ${sqlGetProfileData} )
                           group  BY trunc(f_created_time)
                           order by trunc(f_created_time) asc"""
      sql
    } else if (numDays > 30 && numDays < 90) {
      val sql = s"""select count(*), trunc(f_created_time,'ww') from facebook_results i
                           where f_created_time between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS') and
                           fk_query_id in (select q_id from queries where   ${sqlGetProfileData} )
                           group  BY trunc(f_created_time,'ww')
                           order by trunc(f_created_time, 'ww') asc"""
      sql
    } else {
      val sql = s"""select count(*), trunc(f_created_time,'month') from facebook_results i
                           where f_created_time between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS') and
                           fk_query_id in (select q_id from queries where   ${sqlGetProfileData} )
                           group  BY trunc(f_created_time,'month')
                           order by trunc(f_created_time, 'month') asc"""
      sql
    }
  }


}