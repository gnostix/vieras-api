package gr.gnostix.api.models

import gr.gnostix.api.db.plainsql.DatabaseAccessSupport
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory

import scala.concurrent.{Promise, Future, ExecutionContext}
import scala.slick.jdbc.{StaticQuery => Q, GetResult}

/**
 * Created by rebel on 27/11/14.
 */
object GeoLocationDao extends DatabaseAccessSupport {

  implicit val getCountriesResult = GetResult(r => CountriesLine(r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)


  def getDataByProfileId(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int): Future[Option[List[CountriesLine]]] = {
    val sql = buildQueryByProfileId(profileId, fromDate, toDate)
    //bring the actual data
    val prom = Promise[Option[List[CountriesLine]]]()

    Future {
      prom.success(getData(sql))

    }

    prom.future
  }


  def getDataByDatasourceId(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, datasourceId: Int): Future[Option[List[CountriesLine]]] = {
    val sql = buildQueryByDatasourceId(profileId, datasourceId, fromDate, toDate)
    //bring the actual data
    val prom = Promise[Option[List[CountriesLine]]]()

    Future {
      prom.success(getData(sql))

    }

    prom.future
  }

  def getDataByCredentialsId(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, credId: Int): Future[Option[List[CountriesLine]]] = {
    val sql = buildQueryByCredId(profileId, credId, fromDate, toDate)
    //bring the actual data
    val prom = Promise[Option[List[CountriesLine]]]()

    Future {
      prom.success(getData(sql))

    }

    prom.future
  }



  private def buildQueryByProfileId(profileId: Int, fromDate: DateTime, toDate: DateTime ): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sql =
      s"""
      SELECT * FROM (
        select VIERAS_COUNTRY, COUNT(*) from ENG_HOTEL_REVIEWS
          where FK_HOTEL_ID IN (SELECT FK_HOTEL_ID FROM ENG_PROFILE_HOTEL_CREDENTIALS WHERE FK_PROFILE_ID =${profileId})
            and REVIEW_DATE between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
            and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
        Group by VIERAS_COUNTRY
        order by COUNT(*) DEsc) RESULT
        WHERE ROWNUM <=10
       """

    sql
  }

  private def buildQueryByDatasourceId(profileId: Int, datasourceId: Int, fromDate: DateTime, toDate: DateTime ): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sql =
      s"""
        SELECT * FROM (
        select VIERAS_COUNTRY, COUNT(*) from ENG_HOTEL_REVIEWS
          where FK_HOTEL_ID IN (SELECT FK_HOTEL_ID FROM ENG_PROFILE_HOTEL_CREDENTIALS
              WHERE  FK_DATASOURCE_ID=${datasourceId} and fk_profile_id=${profileId} )
            and REVIEW_DATE between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
            and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
        Group by VIERAS_COUNTRY
        order by COUNT(*) DEsc) RESULT
        WHERE ROWNUM <=10
       """

    sql
  }

  private def buildQueryByCredId(profileId: Int, credId: Int, fromDate: DateTime, toDate: DateTime ): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sql =
      s"""
        SELECT * FROM (
        select VIERAS_COUNTRY, COUNT(*) from ENG_HOTEL_REVIEWS
          where FK_HOTEL_ID IN (SELECT FK_HOTEL_ID FROM ENG_PROFILE_HOTEL_CREDENTIALS
              WHERE  ID=${credId} and fk_profile_id=${profileId} )
            and REVIEW_DATE between TO_DATE('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
            and TO_DATE('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
        Group by VIERAS_COUNTRY
        order by COUNT(*) DEsc) RESULT
        WHERE ROWNUM <=10
       """

    sql
  }

  private def getData(sql: String ): Option[List[CountriesLine]] = {

    try {
      var myDataTotal = List[CountriesLine]()
      getConnection withSession {
        implicit session =>
          logger.info("get countries by msgNum line  ------------->" + sql)
          val records = Q.queryNA[CountriesLine](sql)
          myDataTotal = records.list
      }


      Some(myDataTotal)
    } catch {
      case e: Exception => None
    }
  }


}
