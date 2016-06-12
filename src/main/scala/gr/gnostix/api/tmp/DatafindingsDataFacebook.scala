package gr.gnostix.api.tmp

import gr.gnostix.api.db.plainsql.DatabaseAccessSupportOra
import gr.gnostix.api.models.plainModels.{DataFacebookGraph, SocialData}
import gr.gnostix.api.utilities.{DateUtils, SqlUtils}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory

import scala.slick.jdbc.{GetResult, StaticQuery => Q}


object DtFacebookDataGraphDAO extends DatabaseAccessSupportOra {

  implicit val getDtFacebookLineGraphResult = GetResult(r => DataFacebookGraph(r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)

  def getLineDataDefault(fromDate: DateTime, toDate: DateTime, userId: Int, profileId: Int): SocialData = {
    val mySqlDynamic = SqlUtils.getDataDefaultObj(profileId)
    //bring the actual data
    getLineData(fromDate, toDate, userId, profileId, mySqlDynamic)
  }

  def getLineDataByKeywords(fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  keywords: List[Int]): SocialData = {
    val mySqlDynamic = SqlUtils.getDataByKeywordsObj(userId, profileId, keywords)
    //bring the actual data
    getLineData(fromDate, toDate, userId, profileId, mySqlDynamic)
  }

  def getLineDataByTopics(fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  topics: List[Int]): SocialData = {
    val mySqlDynamic = SqlUtils.getDataByTopicsObj(userId, profileId, topics)
    //bring the actual data
    getLineData(fromDate, toDate, userId, profileId, mySqlDynamic)
  }

  def getLineData(fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  sqlDynamicKeywordsTopics: String) = {

    val sqlQ = buildQuery(fromDate, toDate, userId, profileId, sqlDynamicKeywordsTopics)
    var myData = List[DataFacebookGraph]()

    getConnection withSession {
      implicit session =>
        logger.info("getLineData ------------->" + sqlQ)
        val records = Q.queryNA[DataFacebookGraph](sqlQ)
        myData = records.list()
    }
    val lineData = SocialData("facebook", myData)
    lineData
  }


  def buildQuery(fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  sqlDynamicKeywordsTopics: String): String = {
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