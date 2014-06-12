package gr.gnostix.api.models

import java.sql.Timestamp
import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import scala.slick.jdbc.{GetResult, StaticQuery => Q}
import org.joda.time.{Days, DateTime}
import org.slf4j.LoggerFactory
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import gr.gnostix.api.utilities.DateUtils


object DtWebLineGraphDAO extends DatabaseAccessSupport {

  implicit val getDtWebLineGraphResult = GetResult(r => DataLineGraph(r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)

  def getLineData(fromDate: DateTime, toDate: DateTime, profileId: Int) = {

    val sqlQ = buildQuery(fromDate, toDate, profileId)
    var myData = List[DataLineGraph]()

    getConnection withSession {
      implicit session =>
        logger.info("getLineData ------------->" + sqlQ)
        val records = Q.queryNA[DataLineGraph](sqlQ)
        myData = records.list()
    }
    val lineData = SocialData("web", myData)
    lineData
  }


  def buildQuery(fromDate: DateTime, toDate: DateTime, profileId: Int): String = {
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
      val sql = s"""select count(*), trunc(item_date,'HH') from web_results i
                           where item_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')  and fk_grp_id  = 10 and
                           fk_queries_id in (select q_id from queries where fk_k_id in
                           (select k_id from KEYWORDS where fk_sd_id in (select sd_id from SEARCH_DOMAINS where fk_customer_id=${profileId})))
                           group  BY trunc(item_date,'HH')
                           order by trunc(item_date, 'HH') asc"""
      logger.info("------------>" + sql)
      sql
    } else if (numDays > 1 && numDays <= 30) {
      val sql = s"""select count(*), trunc(item_date) from web_results i
                           where item_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY')   and fk_grp_id  = 10 and
                           fk_queries_id in (select q_id from queries where fk_k_id in
                           (select k_id from KEYWORDS where fk_sd_id in (select sd_id from SEARCH_DOMAINS where fk_customer_id=${profileId})))
                           group  BY trunc(item_date)
                           order by trunc(item_date) asc"""
      sql
    } else if (numDays > 30 && numDays < 90) {
      val sql = s"""select count(*), trunc(item_date,'ww') from web_results i
                           where item_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY')   and fk_grp_id  = 10 and
                           fk_queries_id in (select q_id from queries where fk_k_id in
                           (select k_id from KEYWORDS where fk_sd_id in (select sd_id from SEARCH_DOMAINS where fk_customer_id=${profileId})))
                           group  BY trunc(item_date,'ww')
                           order by trunc(item_date, 'ww') asc"""
      sql
    } else {
      val sql = s"""select count(*), trunc(item_date,'month') from web_results i
                           where item_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY')   and fk_grp_id  = 10 and
                           fk_queries_id in (select q_id from queries where fk_k_id in
                           (select k_id from KEYWORDS where fk_sd_id in (select sd_id from SEARCH_DOMAINS where fk_customer_id=${profileId})))
                           group  BY trunc(item_date,'month')
                           order by trunc(item_date, 'month') asc"""
      sql
    }
  }



}
