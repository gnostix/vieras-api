package gr.gnostix.api.models.publicSearch

import gr.gnostix.api.db.plainsql.DatabaseAccessSupportOra
import gr.gnostix.api.models.plainModels.{DataLineGraph, SocialData}
import gr.gnostix.api.utilities.{DateUtils, SqlUtils}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory

import scala.slick.jdbc.{GetResult, StaticQuery => Q}


object DtYoutubeLineGraphDAO extends DatabaseAccessSupportOra {

  implicit val getDtYoutubeLineGraphResult = GetResult(r => DataLineGraph(r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)


  def getLineDataDefault(fromDate: DateTime, toDate: DateTime, userId: Int, profileId: Int): SocialData = {
    val mySqlDynamic = SqlUtils.getDataDefaultObj(profileId)
    //bring the actual data
    getLineData(fromDate,toDate,userId,profileId,mySqlDynamic)
  }

  def getLineDataByKeywords(fromDate: DateTime, toDate: DateTime, userId :Int, profileId: Int,  keywords: List[Int]): SocialData = {
    val mySqlDynamic = SqlUtils.getDataByKeywordsObj(userId,profileId,keywords)
    //bring the actual data
    getLineData(fromDate,toDate,userId,profileId,mySqlDynamic)
  }

  def getLineDataByTopics(fromDate: DateTime, toDate: DateTime, userId :Int, profileId: Int,  topics: List[Int]): SocialData = {
    val mySqlDynamic = SqlUtils.getDataByTopicsObj(userId,profileId,topics)
    //bring the actual data
    getLineData(fromDate,toDate,userId,profileId,mySqlDynamic)
  }


  def getLineData(fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  sqlDynamicKeywordsTopics: String) = {

    val sqlQ = buildQuery(fromDate, toDate, userId, profileId,sqlDynamicKeywordsTopics)
    var myData = List[DataLineGraph]()

    getConnection withSession {
      implicit session =>
        logger.info("getLineData ------------->" + sqlQ)
        val records = Q.queryNA[DataLineGraph](sqlQ)
        myData = records.list()
    }
    val lineData = SocialData("youtube", myData)
    lineData
  }


  def buildQuery(fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  sqlDynamicKeywordsTopics: String): String = {
    logger.info("-------------> buildQuery -----------")

    val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
    logger.info("------------->" + numDays + "-----------")
    var datePattern: String = ""

      datePattern = "dd-MM-yyyy HH:mm:ss"


    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    getSql(numDays, fromDateStr, toDateStr, sqlDynamicKeywordsTopics)
  }

  def getSql(numDays: Int, fromDateStr: String, toDateStr: String, sqlGetProfileData: String) = {

    if (numDays == 0) {
      val sql = s"""select count(*), trunc(Y_PUBLISHED_AT,'HH') from youtube_results i
                           where Y_PUBLISHED_AT between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS') and
                           fk_query_id in (select q_id from queries where  ${sqlGetProfileData} )
                           group  BY trunc(Y_PUBLISHED_AT,'HH')
                           order by trunc(Y_PUBLISHED_AT, 'HH') asc"""
      logger.info("------------>" + sql)
      sql
    } else if (numDays >= 1 && numDays <= 30) {
      val sql = s"""select count(*), trunc(Y_PUBLISHED_AT) from youtube_results i
                           where Y_PUBLISHED_AT between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS') and
                           fk_query_id in (select q_id from queries where  ${sqlGetProfileData} )
                           group  BY trunc(Y_PUBLISHED_AT)
                           order by trunc(Y_PUBLISHED_AT) asc"""
      sql
    } else if (numDays > 30 && numDays < 90) {
      val sql = s"""select count(*), trunc(Y_PUBLISHED_AT,'ww') from youtube_results i
                           where Y_PUBLISHED_AT between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS') and
                           fk_query_id in (select q_id from queries where  ${sqlGetProfileData} )
                           group  BY trunc(Y_PUBLISHED_AT,'ww')
                           order by trunc(Y_PUBLISHED_AT, 'ww') asc"""
      sql
    } else {
      val sql = s"""select count(*), trunc(Y_PUBLISHED_AT,'month') from youtube_results i
                           where Y_PUBLISHED_AT between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS') and
                           fk_query_id in (select q_id from queries where  ${sqlGetProfileData} )
                           group  BY trunc(Y_PUBLISHED_AT,'month')
                           order by trunc(Y_PUBLISHED_AT, 'month') asc"""
      sql
    }
  }



}
