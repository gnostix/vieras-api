package gr.gnostix.api.models

import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import gr.gnostix.api.utilities.{DateUtils, SqlUtils}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory
import scala.concurrent.{Promise, Future, ExecutionContext}
import scala.slick.jdbc.{StaticQuery => Q, GetResult}


import scala.slick.jdbc.GetResult

/**
 * Created by rebel on 21/10/14.
 */
object MySocialChannelDaoFB extends DatabaseAccessSupport {
  implicit val getSentimentLineResult = GetResult(r => DataLineGraph(r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)


  def getLineCounts(fromDate: DateTime, toDate: DateTime, profileId: Int, dataType: String): Option[SocialData] = {
     val sql = buildQuery(fromDate, toDate, profileId, dataType)

    //bring the actual data
    val data = getData(fromDate, toDate, dataType, profileId, sql)
    data match {
      case Some(data) => Some(data)
      case None => None
    }
  }

  def getLineCountsByQueryId(fromDate: DateTime, toDate: DateTime, profileId: Int, dataType: String, queryId: Int): Option[SocialData] = {
    val sql = buildQuery(fromDate, toDate, profileId, dataType)

    //bring the actual data
    val data = getData(fromDate, toDate, dataType, profileId, sql)
    data match {
      case Some(data) => Some(data)
      case None => None
    }
  }

  def getLineAllData(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, dataType: String): Future[Option[SocialData]] = {
    val mySqlDynamic = SqlUtils.getDataDefaultObj(profileId)
    //bring the actual data
    val prom = Promise[Option[SocialData]]()

    Future {
      prom.success (getData(fromDate, toDate, dataType, profileId, mySqlDynamic) )
    }
    prom.future
  }

  private def getData(fromDate: DateTime, toDate: DateTime, dataType: String, profileId: Int, sql: String): Option[SocialData] = {

    var myData = List[DataLineGraph]()

    getConnection withSession {
      implicit session =>
        logger.info("get my social channel fb ------------->" + sql)
        val records = Q.queryNA[DataLineGraph](sql)
        myData = records.list()
    }
    val lineData = SocialData("facebook", myData)

    lineData match {
      case SocialData(_,_) => Option(lineData)
     }
  }

  def buildQuery(fromDate: DateTime, toDate: DateTime, profileId: Int, dataType: String): String = {

    val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
    logger.info("------------->" + numDays + "-----------")

    val datePattern = "dd-MM-yyyy HH:mm:ss"


    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    dataType match {
      case "post" => getSqlPosts(numDays, fromDateStr, toDateStr, profileId)
      case "comment" => getSqlComments(numDays, fromDateStr, toDateStr)
      case "all" => getSqlComments(numDays, fromDateStr, toDateStr)
    }

  }

  def getSqlPosts(numDays: Int, fromDateStr: String, toDateStr: String, profileId: Int) = {
    if (numDays == 0) {
      val sql = s"""select count(*),trunc(msg_date,'HH') from eng_fb_wall,customers
                      where fk_eng_engagement_data_quer_id in ( select q.id from eng_engagement_data_queries q
                        where q.is_active = 1 and q.attr = 'FB_FANPAGE_WALL'
                        and fk_cust_social_engagement_id in ( select s.id from eng_cust_social_credentials s where s.fk_cust_id in (
                            select customer_id from customers where customer_id =16) and s.fk_datasource_id = 1))
                        and msg_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                        and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS') and customer_id=$profileId
                    group by trunc(msg_date,'HH')
                    order by trunc(msg_date,'HH')asc"""
      logger.info("------------>" + sql)
      sql
    } else if (numDays >= 1 && numDays <= 30) {
      val sql = s"""select count(*),trunc(msg_date) from eng_fb_wall,customers
                      where fk_eng_engagement_data_quer_id in ( select q.id from eng_engagement_data_queries q
                        where q.is_active = 1 and q.attr = 'FB_FANPAGE_WALL'
                        and fk_cust_social_engagement_id in ( select s.id from eng_cust_social_credentials s where s.fk_cust_id in (
                            select customer_id from customers where customer_id =16) and s.fk_datasource_id = 1))
                        and msg_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                        and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS') and customer_id=$profileId
                    group by trunc(msg_date)
                    order by trunc(msg_date)asc"""

      sql
    } else if (numDays > 30 && numDays < 90) {
      val sql = s"""select count(*),trunc(msg_date,'ww') from eng_fb_wall,customers
                      where fk_eng_engagement_data_quer_id in ( select q.id from eng_engagement_data_queries q
                        where q.is_active = 1 and q.attr = 'FB_FANPAGE_WALL'
                        and fk_cust_social_engagement_id in ( select s.id from eng_cust_social_credentials s where s.fk_cust_id in (
                            select customer_id from customers where customer_id =16) and s.fk_datasource_id = 1))
                        and msg_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                        and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS') and customer_id=$profileId
                    group by trunc(msg_date,'ww')
                    order by trunc(msg_date,'ww')asc"""
      sql
    } else {
      val sql = s"""select count(*),trunc(msg_date,'month') from eng_fb_wall,customers
                      where fk_eng_engagement_data_quer_id in ( select q.id from eng_engagement_data_queries q
                        where q.is_active = 1 and q.attr = 'FB_FANPAGE_WALL'
                        and fk_cust_social_engagement_id in ( select s.id from eng_cust_social_credentials s where s.fk_cust_id in (
                            select customer_id from customers where customer_id =16) and s.fk_datasource_id = 1))
                        and msg_date between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                        and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS') and customer_id=$profileId
                    group by trunc(msg_date,'month')
                    order by trunc(msg_date,'month')asc"""
      sql
    }
  }

  def getSqlComments(numDays: Int, fromDateStr: String, toDateStr: String) = {

    "koko"
  }

}
