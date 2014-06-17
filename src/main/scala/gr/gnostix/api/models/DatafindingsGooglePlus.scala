package gr.gnostix.api.models

import java.sql.Timestamp
import scala.slick.jdbc.{GetResult, StaticQuery => Q}
import org.joda.time.{Days, DateTime}
import org.slf4j.LoggerFactory
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import gr.gnostix.api
import gr.gnostix.api.utilities.{SqlUtils, DateUtils}
import gr.gnostix.api.db.plainsql.DatabaseAccessSupport


object DtGoogleplusLineGraphDAO extends DatabaseAccessSupport {

  implicit val getDtGoogleplusLineGraphResult = GetResult(r => DataLineGraph(r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)

  def getLineDataDefault(fromDate: DateTime, toDate: DateTime, profileId: Int): SocialData = {
    val mySqlDynamic = SqlUtils.getLineDataDefaultObj(fromDate,toDate,profileId)
    //bring the actual data
    getLineData(fromDate,toDate,profileId,mySqlDynamic)
  }

  def getLineDataByKeywords(fromDate: DateTime, toDate: DateTime, profileId: Int, keywords: List[Int]): SocialData = {
    val mySqlDynamic = SqlUtils.getLineDataByKeywordsObj(fromDate,toDate,profileId,keywords)
    //bring the actual data
    getLineData(fromDate,toDate,profileId,mySqlDynamic)
  }

  def getLineDataByTopics(fromDate: DateTime, toDate: DateTime, profileId: Int, topics: List[Int]): SocialData = {
    val mySqlDynamic = SqlUtils.getLineDataByTopicsObj(fromDate,toDate,profileId,topics)
    //bring the actual data
    getLineData(fromDate,toDate,profileId,mySqlDynamic)
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
    val lineData = SocialData("googleplus", myData)
    lineData
  }


  def buildQuery(fromDate: DateTime, toDate: DateTime, profileId: Int, sqlDynamicKeywordsTopics: String): String = {
    logger.info("-------------> buildQuery -----------")

    val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
    logger.info("------------->" + numDays + "-----------")
    var datePattern: String = ""

    if (numDays == 0) {
      datePattern = "dd-MM-yyyy HH:mm:ss"
    } else {
      datePattern = "dd-MM-yyyy"
    }

    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)


    getSql(numDays, fromDateStr, toDateStr, sqlDynamicKeywordsTopics)
  }

  def getSql(numDays: Int, fromDateStr: String, toDateStr: String, sqlGetProfileData: String) = {
    if (numDays == 0) {
      val sql = s"""select count(*), trunc(itemdate,'HH') from googleplus_results i
                           where itemdate between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS') and
                           fk_query_id in (select q_id from queries where  ${sqlGetProfileData} )
                           group  BY trunc(itemdate,'HH')
                           order by trunc(itemdate, 'HH') asc"""
      logger.info("------------>" + sql)
      sql
    } else if (numDays > 1 && numDays <= 30) {
      val sql = s"""select count(*), trunc(itemdate) from googleplus_results i
                           where itemdate between TO_DATE('${fromDateStr}', 'DD-MM-YYYY')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY') and
                           fk_query_id in (select q_id from queries where  ${sqlGetProfileData} )
                           group  BY trunc(itemdate)
                           order by trunc(itemdate) asc"""
      sql
    } else if (numDays > 30 && numDays < 90) {
      val sql = s"""select count(*), trunc(itemdate,'ww') from googleplus_results i
                           where itemdate between TO_DATE('${fromDateStr}', 'DD-MM-YYYY')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY') and
                           fk_query_id in (select q_id from queries where  ${sqlGetProfileData} )
                           group  BY trunc(itemdate,'ww')
                           order by trunc(itemdate, 'ww') asc"""
      sql
    } else {
      val sql = s"""select count(*), trunc(itemdate,'month') from googleplus_results i
                           where itemdate between TO_DATE('${fromDateStr}', 'DD-MM-YYYY')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY') and
                           fk_query_id in (select q_id from queries where  ${sqlGetProfileData} )
                           group  BY trunc(itemdate,'month')
                           order by trunc(itemdate, 'month') asc"""
      sql
    }
  }



}