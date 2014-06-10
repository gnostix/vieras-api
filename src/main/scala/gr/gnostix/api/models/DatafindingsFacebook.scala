package gr.gnostix.api.models

import java.sql.Timestamp
import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import scala.slick.jdbc.{GetResult, StaticQuery => Q}
import org.joda.time.{Days, DateTime}
import org.slf4j.LoggerFactory
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import gr.gnostix.api.utilities.DateUtils


object DtFacebookLineGraphDAO extends DatabaseAccessSupport {

  implicit val getDtFacebookLineGraphResult = GetResult(r => DataLineGraph(r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)

  val twSqlLineByDay = """select count(*), trunc(f_created_time,'HH') from Facebook_results i
                           where f_created_time between TO_DATE('2014-02-27', 'YYYY/MM/DD')
                           and TO_DATE('2014-02-27', 'YYYY/MM/DD')
                           group  BY trunc(f_created_time,'HH')
                           order by trunc(f_created_time, 'HH') asc"""

  def getLineDataByDay = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[DataLineGraph](twSqlLineByDay)
        records.list()
    }
  }

  def getLineData(fromDate: DateTime, toDate: DateTime) = {

    val sqlQ = buildQuery(fromDate, toDate)
    var myData = List[DataLineGraph]()

    getConnection withSession {
      implicit session =>
        println("getTWLineData ------------->" + sqlQ)
        val records = Q.queryNA[DataLineGraph](sqlQ)
        myData = records.list()
    }
    val lineData = SocialData("Facebook", myData)
    lineData
  }


  def buildQuery(fromDate: DateTime, toDate: DateTime): String = {
    println("-------------> buildTwQuery -----------")

    val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
    println("------------->" + numDays + "-----------")
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
      val sql = s"""select count(*), trunc(f_created_time,'HH') from facebook_results i
                           where f_created_time between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS') and
                           fk_query_id in (select q_id from queries where fk_k_id in
                           (select k_id from KEYWORDS where fk_sd_id in (select sd_id from SEARCH_DOMAINS where fk_customer_id=10)))
                           group  BY trunc(f_created_time,'HH')
                           order by trunc(f_created_time, 'HH') asc"""
      println("------------>" + sql)
      sql
    } else if (numDays > 1 && numDays <= 30) {
      val sql = s"""select count(*), trunc(f_created_time) from facebook_results i
                           where f_created_time between TO_DATE('${fromDateStr}', 'DD-MM-YYYY')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY') and
                           fk_query_id in (select q_id from queries where fk_k_id in
                           (select k_id from KEYWORDS where fk_sd_id in (select sd_id from SEARCH_DOMAINS where fk_customer_id=10)))
                           group  BY trunc(f_created_time)
                           order by trunc(f_created_time) asc"""
      sql
    } else if (numDays > 30 && numDays < 90) {
      val sql = s"""select count(*), trunc(f_created_time,'ww') from facebook_results i
                           where f_created_time between TO_DATE('${fromDateStr}', 'DD-MM-YYYY')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY') and
                           fk_query_id in (select q_id from queries where fk_k_id in
                           (select k_id from KEYWORDS where fk_sd_id in (select sd_id from SEARCH_DOMAINS where fk_customer_id=10)))
                           group  BY trunc(f_created_time,'ww')
                           order by trunc(f_created_time, 'ww') asc"""
      sql
    } else {
      val sql = s"""select count(*), trunc(f_created_time,'month') from facebook_results i
                           where f_created_time between TO_DATE('${fromDateStr}', 'DD-MM-YYYY')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY') and
                           fk_query_id in (select q_id from queries where fk_k_id in
                           (select k_id from KEYWORDS where fk_sd_id in (select sd_id from SEARCH_DOMAINS where fk_customer_id=10)))
                           group  BY trunc(f_created_time,'month')
                           order by trunc(f_created_time, 'month') asc"""
      sql
    }
  }



}