package gr.gnostix.api.models.publicSearch

import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import gr.gnostix.api.models.plainModels.{SocialData, DataLineGraph}
import gr.gnostix.api.utilities.{DateUtils, SqlUtils}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory

import scala.slick.jdbc.{GetResult, StaticQuery => Q}

object WebDatasources {
  // this is the id from datasources for news portals and the same for the next feed categories
  val web = Map(8 -> "web")
  val linkedin = Map(10 -> "linkedin")
}

object DtWebLineGraphDAO extends DatabaseAccessSupport {

  implicit val getDtWebLineGraphResult = GetResult(r => DataLineGraph(r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)

  def getLineDataDefault(fromDate: DateTime, toDate: DateTime, profileId: Int, webSourceType: Map[Int, String]): SocialData = {
    val mySqlDynamic = SqlUtils.getDataDefaultObj(profileId)
    //bring the actual data
    getLineData(fromDate, toDate, profileId, webSourceType, mySqlDynamic)
  }

  def getLineDataByKeywords(fromDate: DateTime, toDate: DateTime, profileId: Int, keywords: List[Int], webSourceType: Map[Int, String]): SocialData = {
    val mySqlDynamic = SqlUtils.getDataByKeywordsObj(profileId, keywords)
    //bring the actual data
    getLineData(fromDate, toDate, profileId, webSourceType, mySqlDynamic)
  }

  def getLineDataByTopics(fromDate: DateTime, toDate: DateTime, profileId: Int, topics: List[Int], webSourceType: Map[Int, String]): SocialData = {
    val mySqlDynamic = SqlUtils.getDataByTopicsObj(profileId, topics)
    //bring the actual data
    getLineData(fromDate, toDate, profileId, webSourceType, mySqlDynamic)
  }

  def getLineData(fromDate: DateTime, toDate: DateTime, profileId: Int, webSourceType: Map[Int, String], sqlDynamicKeywordsTopics: String) = {

    val sqlQ = buildQuery(fromDate, toDate, profileId, webSourceType.head._1, sqlDynamicKeywordsTopics)
    var myData = List[DataLineGraph]()

    getConnection withSession {
      implicit session =>
        logger.info("getLineData ------------->" + sqlQ)
        val records = Q.queryNA[DataLineGraph](sqlQ)
        myData = records.list()
    }
    val lineData = webSourceType.head._2 match {
      case "web" => SocialData("web", myData)
      case "linkedin" => SocialData("linkedin", myData)
    }
    lineData
  }


  def buildQuery(fromDate: DateTime, toDate: DateTime, profileId: Int, webSourceId: Int, sqlDynamicKeywordsTopics: String): String = {
    logger.info("-------------> buildQuery -----------")

    val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
    logger.info("------------->" + numDays + "-----------")
 
    val datePattern = "dd-MM-yyyy HH:mm:ss"
 

    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)
    getSql(numDays, fromDateStr, toDateStr, profileId, webSourceId, sqlDynamicKeywordsTopics)
  }


  def getSql(numDays: Int, fromDateStr: String, toDateStr: String, profileId: Int, webSourceId: Int, sqlGetProfileData: String) = {

    if (numDays == 0) {
      val sql = s"""select count(*), trunc(item_date,'HH') from web_results i
                           where item_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')  and fk_grp_id  = ${webSourceId} and
                           fk_queries_id in (select q_id from queries where ${sqlGetProfileData} )
                           group  BY trunc(item_date,'HH')
                           order by trunc(item_date, 'HH') asc"""
      logger.info("------------>" + sql)
      sql
    } else if (numDays >= 1 && numDays <= 30) {
      val sql = s"""select count(*), trunc(item_date) from web_results i
                           where item_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')   and fk_grp_id  = ${webSourceId}  and
                           fk_queries_id in (select q_id from queries where ${sqlGetProfileData} )
                           group  BY trunc(item_date)
                           order by trunc(item_date) asc"""
      sql
    } else if (numDays > 30 && numDays < 90) {
      val sql = s"""select count(*), trunc(item_date,'ww') from web_results i
                           where item_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')   and fk_grp_id  = ${webSourceId}  and
                           fk_queries_id in (select q_id from queries where ${sqlGetProfileData} )
                           group  BY trunc(item_date,'ww')
                           order by trunc(item_date, 'ww') asc"""
      sql
    } else {
      val sql = s"""select count(*), trunc(item_date,'month') from web_results i
                           where item_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')   and fk_grp_id  = ${webSourceId}  and
                           fk_queries_id in (select q_id from queries where ${sqlGetProfileData} )
                           group  BY trunc(item_date,'month')
                           order by trunc(item_date, 'month') asc"""
      sql
    }
  }


}
