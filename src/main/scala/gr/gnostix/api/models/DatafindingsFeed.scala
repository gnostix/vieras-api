package gr.gnostix.api.models

import java.sql.Timestamp
import scala.slick.jdbc.{GetResult, StaticQuery => Q}
import org.joda.time.{Days, DateTime}
import org.slf4j.LoggerFactory
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import gr.gnostix.api
import gr.gnostix.api.utilities.DateUtils
import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import gr.gnostix.api.utilities.DateUtils

object FeedDatasources {
  // this is the id from datasources for news portals and the same for the next feed categories
  val news = Map(5 -> "news")
  val blogs = Map(3 -> "blogs")
  val personal = Map(13 -> "personal")
}

object DtFeedLineGraphDAO extends DatabaseAccessSupport {

  implicit val getDtFeedLineGraphResult = GetResult(r => DataLineGraph(r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)


  def getLineData(fromDate: DateTime, toDate: DateTime, profileId: Int, feedType: Map[Int, String]):SocialData = {

    val sqlQ = buildQuery(fromDate, toDate, profileId, feedType.head._1)
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


  def buildQuery(fromDate: DateTime, toDate: DateTime, profileId: Int, feedType: Int): String = {
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

    if (numDays == 0) {
      val sql = s"""select count(*), trunc(RSS_DATE,'HH') from feed_results i
                           where RSS_DATE between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')   and fk_grp_id  = ${feedType} and
                           fk_queries_id in (select q_id from queries where fk_k_id in
                           (select k_id from KEYWORDS where fk_sd_id in (select sd_id from SEARCH_DOMAINS where fk_customer_id=${profileId})))
                           group  BY trunc(RSS_DATE,'HH')
                           order by trunc(RSS_DATE, 'HH') asc"""
      logger.info("------------>" + sql)
      sql
    } else if (numDays > 1 && numDays <= 30) {
      val sql = s"""select count(*), trunc(RSS_DATE) from feed_results i
                           where RSS_DATE between TO_DATE('${fromDateStr}', 'DD-MM-YYYY')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY')   and fk_grp_id  = ${feedType} and
                           fk_queries_id in (select q_id from queries where fk_k_id in
                           (select k_id from KEYWORDS where fk_sd_id in (select sd_id from SEARCH_DOMAINS where fk_customer_id=${profileId})))
                           group  BY trunc(RSS_DATE)
                           order by trunc(RSS_DATE) asc"""
      sql
    } else if (numDays > 30 && numDays < 90) {
      val sql = s"""select count(*), trunc(RSS_DATE,'ww') from feed_results i
                           where RSS_DATE between TO_DATE('${fromDateStr}', 'DD-MM-YYYY')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY')   and fk_grp_id  = ${feedType} and
                           fk_queries_id in (select q_id from queries where fk_k_id in
                           (select k_id from KEYWORDS where fk_sd_id in (select sd_id from SEARCH_DOMAINS where fk_customer_id=${profileId})))
                           group  BY trunc(RSS_DATE,'ww')
                           order by trunc(RSS_DATE, 'ww') asc"""
      sql
    } else {
      val sql = s"""select count(*), trunc(RSS_DATE,'month') from feed_results i
                           where RSS_DATE between TO_DATE('${fromDateStr}', 'DD-MM-YYYY')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY')   and fk_grp_id  = ${feedType} and
                           fk_queries_id in (select q_id from queries where fk_k_id in
                           (select k_id from KEYWORDS where fk_sd_id in (select sd_id from SEARCH_DOMAINS where fk_customer_id=${profileId})))
                           group  BY trunc(RSS_DATE,'month')
                           order by trunc(RSS_DATE, 'month') asc"""
      sql
    }
  }


}