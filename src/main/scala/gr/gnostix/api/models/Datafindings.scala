package gr.gnostix.api.models

import java.sql.Timestamp
import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import scala.slick.jdbc.{GetResult, StaticQuery => Q}
import org.joda.time.{Days, DateTime}
import org.slf4j.LoggerFactory
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}


case class DtTwitterLineGraph(tweetsNumb: Int, tweetDate: Timestamp)

object DtTwitterLineGraphDAO extends DatabaseAccessSupport {

  implicit val getDtTwitterLineGraphResult = GetResult(r => DtTwitterLineGraph(r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)

  val twSqlLineByDay = """select count(*), trunc(t_created_at,'HH') from twitter_results i
                           where t_created_at between TO_DATE('2014-02-27', 'YYYY/MM/DD')
                           and TO_DATE('2014-02-27', 'YYYY/MM/DD')
                           group  BY trunc(t_created_at,'HH')
                           order by trunc(t_created_at, 'HH') asc"""

  def getTWLineDataByDay = {
    getConnection withSession {
      implicit session =>
        val records = Q.queryNA[DtTwitterLineGraph](twSqlLineByDay)
        records.list()
    }
  }

  def getTWLineData(fromDate: DateTime, toDate: DateTime) = {

    val sqlQ = buildTwQuery(fromDate, toDate)

    getConnection withSession {
      implicit session =>
        println("------------->" + sqlQ)
        val records = Q.queryNA[DtTwitterLineGraph](sqlQ)
        records.list()
    }
  }


  def buildTwQuery(fromDate: DateTime, toDate: DateTime): String = {

    val numDays = findNumberOfDays(fromDate, toDate)
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
      val sql = s"""select count(*), trunc(t_created_at,'HH') from twitter_results i
                           where t_created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                           group  BY trunc(t_created_at,'HH')
                           order by trunc(t_created_at, 'HH') asc"""
      sql
    } else if (numDays > 1 && numDays <= 30) {
      val sql = s"""select count(*), trunc(t_created_at) from twitter_results i
                           where t_created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY')
                           group  BY trunc(t_created_at)
                           order by trunc(t_created_at) asc"""
      sql
    } else if (numDays > 30 && numDays < 90) {
      val sql = s"""select count(*), trunc(t_created_at,'ww') from twitter_results i
                           where t_created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY')
                           group  BY trunc(t_created_at,'ww')
                           order by trunc(t_created_at, 'ww') asc"""
      sql
    } else {
      val sql = s"""select count(*), trunc(t_created_at,'month') from twitter_results i
                           where t_created_at between TO_DATE('${fromDateStr}', 'DD-MM-YYYY')
                           and TO_DATE('${toDateStr}', 'DD-MM-YYYY')
                           group  BY trunc(t_created_at,'month')
                           order by trunc(t_created_at, 'month') asc"""
      sql
    }
  }

  def findNumberOfDays(fromDate: DateTime, toDate: DateTime): Int = {
    try {
      val days = Days.daysBetween(fromDate, toDate).getDays
      logger.info("-----------------------> number of days between the two dates  " + fromDate + " " + toDate)
      logger.info("-----------------------> number of days between the two dates  " + days)
      days
    } catch {
      case e: Exception => println("-------------- excpetion in findNumberOfDays")
        000
    }
  }

}