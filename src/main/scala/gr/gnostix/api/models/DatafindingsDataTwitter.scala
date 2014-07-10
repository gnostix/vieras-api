package gr.gnostix.api.models

import java.sql.Timestamp
import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import scala.slick.jdbc.{GetResult, StaticQuery => Q}
import org.joda.time.{Days, DateTime}
import org.slf4j.LoggerFactory
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import gr.gnostix.api.utilities.{SqlUtils, DateUtils}


object DatafindingsDataTwitterDAO extends DatabaseAccessSupport {

  implicit val getDtTwitterDataGraphResult = GetResult(r => DataTwitterGraph(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)


  def getRowDataDefault(fromDate: DateTime, toDate: DateTime, profileId: Int): SocialData = {
    val mySqlDynamic = SqlUtils.getDataDefaultObj(profileId)
    //bring the actual data
    getRawData(fromDate, toDate, profileId, mySqlDynamic)
  }

  def getRowDataByKeywords(fromDate: DateTime, toDate: DateTime, profileId: Int, keywords: List[Int]): SocialData = {
    val mySqlDynamic = SqlUtils.getDataByKeywordsObj(profileId, keywords)
    //bring the actual data
    getRawData(fromDate, toDate, profileId, mySqlDynamic)
  }

  def getRowDataByTopics(fromDate: DateTime, toDate: DateTime, profileId: Int, topics: List[Int]): SocialData = {
    val mySqlDynamic = SqlUtils.getDataByTopicsObj(profileId, topics)
    //bring the actual data
    getRawData(fromDate, toDate, profileId, mySqlDynamic)
  }


  def getRawData(fromDate: DateTime, toDate: DateTime, profileId: Int, sqlDynamicKeywordsTopics: String) = {

    val sqlQ = buildQuery(fromDate, toDate, profileId, sqlDynamicKeywordsTopics)
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


  def buildQuery(fromDate: DateTime, toDate: DateTime, profileId: Int, sqlDynamicKeywordsTopics: String): String = {
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
