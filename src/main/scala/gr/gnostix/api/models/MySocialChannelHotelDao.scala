
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
object MySocialChannelHotelDao extends DatabaseAccessSupport {
  implicit val getLineResult = GetResult(r => DataLineGraph(r.<<, r.<<))
  implicit val getTotalResult = GetResult(r => MsgNum(r.<<))

  val logger = LoggerFactory.getLogger(getClass)


  def getDataCounts(fromDate: DateTime, toDate: DateTime, profileId: Int, dataType :String, datasourceId: Option[Int]): Option[Payload] = {
    val sql = buildQuery(fromDate, toDate, profileId, dataType, datasourceId)

    //bring the actual data
    val data = dataType match {
      case "line" => getData(sql)
      case "total" => getDataTotal(dataType, sql)
    }

    data match {
      case Some(data) => Some(data)
      case None => None
    }
  }



  def getDataCountsFuture(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, dataType: String, datasourceId: Option[Int]): Future[Option[SocialData]] = {
    val mySqlDynamic = buildQuery(fromDate, toDate, profileId, dataType, datasourceId)
    //bring the actual data
    val prom = Promise[Option[SocialData]]()

    Future {
      prom.success(getData(mySqlDynamic))
    }
    prom.future
  }



  def getTotalSumDataFuture(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, dataType: String, datasourceId: Option[Int]): Future[Option[SocialDataSum]] = {
    val mySqlDynamic = buildQuery(fromDate, toDate, profileId, dataType, datasourceId)
    //bring the actual data
    val prom = Promise[Option[SocialDataSum]]()

    Future {
      prom.success(getDataTotal(dataType, mySqlDynamic))
    }
    prom.future
  }



  private def getData(sql: String): Option[SocialData] = {

    try {
      var myData = List[DataLineGraph]()
      getConnection withSession {
        implicit session =>
          logger.info("get my social channel hotel ------------->" + sql)
          val records = Q.queryNA[DataLineGraph](sql)
          myData = records.list()
      }

      val lineData = SocialData("hotel", myData)

      lineData match {
        case SocialData(_, _) => Option(lineData)
      }
    } catch {
      case e: Exception => None
    }

  }

  private def getDataTotal(dataType: String, sql: String): Option[SocialDataSum] = {

    try {
      var myDataTotal = 0
      getConnection withSession {
        implicit session =>
          logger.info("get my social channel hotel ------------->" + sql)
          val records = Q.queryNA[Int](sql)
          myDataTotal = records.first()
      }

      val sumData = SocialDataSum("hotel", myDataTotal)

      Option(sumData)
    } catch {
      case e: Exception => None
    }

  }

  /**
   *
   * @param fromDate
   * @param toDate
   * @param profileId
   * @param datasourceId
   * @return a sql query combination of datasource id and credentials id if present on of those or both (pre profile id)
   */
  private def buildQuery(fromDate: DateTime, toDate: DateTime, profileId: Int, dataType:String, datasourceId: Option[Int]): String = {

    val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
    logger.info("------------->" + numDays + "-----------")

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val sqlEngAccount = datasourceId match {
      case Some(x) => s""" SELECT FK_HOTEL_ID FROM ENG_PROFILE_HOTEL_CREDENTIALS WHERE FK_PROFILE_ID = ${profileId} and fk_datasource_id = ${datasourceId} """
      case None => s""" SELECT FK_HOTEL_ID FROM ENG_PROFILE_HOTEL_CREDENTIALS WHERE FK_PROFILE_ID = ${profileId} """

    }
    logger.info("------------->" + sqlEngAccount + "-----------")
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    dataType match {
      case "line" => getSqlHotelDataLine(numDays, fromDateStr, toDateStr, profileId, sqlEngAccount)
      case "total" => getSqlHotelDataTotal(numDays, fromDateStr, toDateStr, profileId, sqlEngAccount)
    }

  }

  private def getSqlHotelDataTotal(numDays: Int, fromDateStr: String, toDateStr: String, profileId: Int, sqlEngAccount: String) = {
    val sql = s"""
        select count(*) from ENG_HOTEL_REVIEWS
          where FK_HOTEL_ID IN ( $sqlEngAccount  )
            and REVIEW_DATE between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
        """
    sql
  }

  private def getSqlHotelDataLine(numDays: Int, fromDateStr: String, toDateStr: String, profileId: Int, sqlEngAccount: String) = {
    val grouBydate = DateUtils.sqlGrouByDate(numDays)

      val sql = s"""
      select count(*),trunc(REVIEW_DATE,'${grouBydate}') from ENG_HOTEL_REVIEWS
          where FK_HOTEL_ID IN ( $sqlEngAccount  )
            and REVIEW_DATE between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
            and trunc(REVIEW_DATE,'${grouBydate}') >= TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
        group by trunc(REVIEW_DATE,'${grouBydate}')
        order by trunc(REVIEW_DATE,'${grouBydate}')asc
                     """
      logger.info("------------>" + sql)
      sql

  }


}


