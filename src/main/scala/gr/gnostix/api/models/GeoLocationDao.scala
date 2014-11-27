package gr.gnostix.api.models

import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory

import scala.concurrent.{Promise, Future, ExecutionContext}
import scala.slick.jdbc.GetResult

/**
 * Created by rebel on 27/11/14.
 */
object GeoLocationDao extends DatabaseAccessSupport {

  implicit val getCountriesResult = GetResult(r => CountriesLine(r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)


  def getDataByProfileId(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, datasource: Option[String]): Future[Option[String]] = {
    val mySqlDynamic = buildQuery(profileId, fromDate, toDate)
    //bring the actual data
    val prom = Promise[Option[String]]()

    Future {
      prom.success(getData(mySqlDynamic))

    }

    prom.future
  }

  private def getData(mySqlDynamic: String ): Option[String] = {

    Some("kokokokoo")
  }

  def buildQuery(profileId: Int, fromDate: DateTime, toDate: DateTime ): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sql =
      s"""
      SELECT * FROM (
        select COUNT(*),VIERAS_COUNTRY from ENG_HOTEL_REVIEWS
          where FK_HOTEL_ID IN (SELECT FK_HOTEL_ID FROM ENG_PROFILE_HOTEL_CREDENTIALS WHERE FK_PROFILE_ID =1)
            and REVIEW_DATE between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS') and TO_DATE('2014-12-07', 'YYYY/MM/DD HH24:MI:SS')
        Group by VIERAS_COUNTRY
        order by COUNT(*) DEsc) RESULT
        WHERE ROWNUM <=10
       """

    sql
  }

}
