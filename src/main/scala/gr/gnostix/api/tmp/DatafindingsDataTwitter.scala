package gr.gnostix.api.tmp

import gr.gnostix.api.db.plainsql.DatabaseAccessSupportOra
import gr.gnostix.api.models.plainModels.{DataTwitterGraph, SocialData}
import gr.gnostix.api.utilities.{DateUtils, SqlUtils}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory

import scala.slick.jdbc.{GetResult, StaticQuery => Q}


object DatafindingsDataTwitterDAO extends DatabaseAccessSupportOra {

  implicit val getDtTwitterDataGraphResult = GetResult(r => DataTwitterGraph(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)


  def getRowDataDefault(fromDate: DateTime, toDate: DateTime, userId: Int, profileId: Int): SocialData = {
    val mySqlDynamic = SqlUtils.getDataDefaultObj(profileId)
    //bring the actual data
    getRawData(fromDate, toDate, userId, profileId, mySqlDynamic)
  }

  def getRowDataByKeywords(fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  keywords: List[Int]): SocialData = {
    val mySqlDynamic = SqlUtils.getDataByKeywordsObj(userId, profileId, keywords)
    //bring the actual data
    getRawData(fromDate, toDate, userId, profileId, mySqlDynamic)
  }

  def getRowDataByTopics(fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  topics: List[Int]): SocialData = {
    val mySqlDynamic = SqlUtils.getDataByTopicsObj(userId, profileId, topics)
    //bring the actual data
    getRawData(fromDate, toDate, userId, profileId, mySqlDynamic)
  }


  def getRawData(fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  sqlDynamicKeywordsTopics: String) = {

    val sqlQ = buildQuery(fromDate, toDate, userId, profileId, sqlDynamicKeywordsTopics)
    var myData = List[DataTwitterGraph]()

    getConnection withSession {
      implicit session =>
        logger.info("getTWRawData ------------->" + sqlQ)
        val records = Q.queryNA[DataTwitterGraph](sqlQ)
        myData = records.list()
    }
    val lineData = SocialData("twitter", myData)
    lineData
  }


  def buildQuery(fromDate: DateTime, toDate: DateTime,userId :Int, profileId: Int,  sqlDynamicKeywordsTopics: String): String = {
    logger.info("-------------> buildTwQuery -----------")

    val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
    logger.info("------------->" + numDays + "-----------")

    val datePattern = "dd-MM-yyyy HH:mm:ss"


    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    getSqlFullData(numDays, fromDateStr, toDateStr, sqlDynamicKeywordsTopics)


  }

  def getSqlFullData(numDays: Int, fromDateStr: String, toDateStr: String, sqlGetProfileData: String) = {
       val sql = s"""select t.tw_id, t_created_at, t.t_from_user,t.t_from_user_id,t.fk_query_id,
                  t.t_tweet_id,t.followers,t.following,t.listed,t.t_text,t.t_profile_image_url, M.SENTIMENT as sentiment
                      from twitter_results t,MSG_ANALYTICS m
                      where TW_ID = M.FK_MSG_ID and show_flag !=0 and fk_query_id in (select q_id from queries where  ${sqlGetProfileData} )
                        and t_created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                  order by t_created_at asc"""
      sql

    //logger.info("------------>" + sql)

  }


}
