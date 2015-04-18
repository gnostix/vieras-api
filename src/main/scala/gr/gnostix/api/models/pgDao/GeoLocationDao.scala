package gr.gnostix.api.models.pgDao

import gr.gnostix.api.db.plainsql.DatabaseAccessSupportPg
import gr.gnostix.api.models.plainModels.{ApiData, HotelTextData, CountriesLine}
import gr.gnostix.api.utilities.SqlUtils
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.slick.jdbc.{GetResult, StaticQuery => Q}

/**
 * Created by rebel on 27/11/14.
 */
object GeoLocationDao extends DatabaseAccessSupportPg {

  implicit val getCountriesResult = GetResult(r => CountriesLine(r.<<, r.<<))
  implicit val getHotelTextData = GetResult(r => HotelTextData(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)


  def getDataByProfileId(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int): Future[Option[List[CountriesLine]]] = {
    val sql = buildQueryByProfileId(profileId, companyId, fromDate, toDate)
    //bring the actual data
    val prom = Promise[Option[List[CountriesLine]]]()

    Future {
      prom.success(getData(sql))

    }

    prom.future
  }

  def getTextDataByProfileId(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int, countryId: String): Future[Option[ApiData]] = {
    val sql = buildQueryTextData(fromDate, toDate, profileId, companyId, None, countryId)
    //bring the actual data
    val prom = Promise[Option[ApiData]]()

    Future {
      prom.success(getTextDataRaw(sql))

    }

    prom.future
  }

  def getDataByDatasourceId(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int,
                            datasourceId: Int): Future[Option[List[CountriesLine]]] = {
    val sql = buildQueryByDatasourceId(profileId, companyId, datasourceId, fromDate, toDate)
    //bring the actual data
    val prom = Promise[Option[List[CountriesLine]]]()

    Future {
      prom.success(getData(sql))

    }

    prom.future
  }

  def getTextDataByDatasourceId(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int,
                                datasourceId: Int, countryId: String): Future[Option[ApiData]] = {
    val sql = buildQueryTextData(fromDate, toDate, profileId, companyId, Some(datasourceId), countryId)
    //bring the actual data
    val prom = Promise[Option[ApiData]]()

    Future {
      prom.success(getTextDataRaw(sql))

    }

    prom.future
  }

  def getDataByCredentialsId(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int,
                             credId: Int): Future[Option[List[CountriesLine]]] = {
    val sql = buildQueryByCredId(profileId, companyId, credId, fromDate, toDate)
    //bring the actual data
    val prom = Promise[Option[List[CountriesLine]]]()

    Future {
      prom.success(getData(sql))

    }

    prom.future
  }


  private def buildQueryByProfileId(profileId: Int, companyId: Int, fromDate: DateTime, toDate: DateTime): String = {

    val sqlEngAccount = SqlUtils.buildHotelCredentialsQuery(profileId, companyId)

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sql =
      s"""
        SELECT * FROM (
        select VIERAS_COUNTRY, COUNT(*) from vieras.ENG_REVIEWS
          where FK_HOTEL_ID IN ( ${sqlEngAccount} )
            and CREATED between
            to_timestamp('${fromDateStr}', 'dd-mm-yyyy hh24:mi:ss') and to_timestamp('${toDateStr}', 'dd-mm-yyyy hh24:mi:ss')
        Group by VIERAS_COUNTRY
        order by COUNT(*) DEsc) RESULT
         LIMIT 9
       """

    sql
  }

  private def buildQueryByDatasourceId(profileId: Int, companyId: Int, datasourceId: Int, fromDate: DateTime, toDate: DateTime): String = {

    val sqlEngAccount = SqlUtils.buildHotelDatasourceQuery(profileId, companyId, datasourceId)

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sql =
      s"""
      SELECT * FROM (
        select VIERAS_COUNTRY, COUNT(*) from vieras.ENG_REVIEWS
          where FK_HOTEL_ID IN ( ${sqlEngAccount} )
            and CREATED between
            to_timestamp('${fromDateStr}', 'dd-mm-yyyy hh24:mi:ss') and to_timestamp('${toDateStr}', 'dd-mm-yyyy hh24:mi:ss')
        Group by VIERAS_COUNTRY
        order by COUNT(*) DEsc) RESULT
         LIMIT 9
       """

    sql
  }


  private def buildQueryByCredId(profileId: Int, companyId: Int, credId: Int, fromDate: DateTime, toDate: DateTime): String = {

    val sqlEngAccount = SqlUtils.buildHotelCredIdQuery(profileId, companyId, credId)

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sql =
      s"""
        SELECT * FROM (
        select VIERAS_COUNTRY, COUNT(*) from vieras.ENG_REVIEWS
          where FK_HOTEL_ID IN ( ${sqlEngAccount} )
            and CREATED between
            to_timestamp('${fromDateStr}', 'dd-mm-yyyy hh24:mi:ss') and to_timestamp('${toDateStr}', 'dd-mm-yyyy hh24:mi:ss')
        Group by VIERAS_COUNTRY
        order by COUNT(*) DEsc) RESULT
         LIMIT 10
       """

    sql
  }

  private def getData(sql: String): Option[List[CountriesLine]] = {

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

  private def getTextDataRaw(sql: String): Option[ApiData] = {
    try {
      var myData = List[HotelTextData]()
      getConnection withSession {
        implicit session =>
          logger.info("get my hotel HotelTextData ------------->" + sql)
          val records = Q.queryNA[HotelTextData](sql)
          myData = records.list()
      }

      Some(ApiData("hotel_messages", myData))

    } catch {
      case e: Exception => {
        e.printStackTrace()
        None
      }
    }
  }

  private def buildQueryTextData(fromDate: DateTime, toDate: DateTime, profileId: Int, companyId: Int, datasourceId: Option[Int], countryId: String): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)

    val sqlEngAccount = datasourceId match {
      case Some(x) => SqlUtils.buildHotelDatasourceQuery(profileId, companyId, datasourceId.get)
      case None => SqlUtils.buildHotelCredentialsQuery(profileId, companyId)
    }


    val sql =         s"""
        select r.id, substring( (r.review_title || '. ' || r.review_text) from 0 for 240),
          r.VIERAS_TOTAL_RATING as vieras_review_rating, cr.fk_hotel_id, dt.ds_name, r.created, h.hotel_url, r.vieras_country, r.stay_type
                from vieras.ENG_REVIEWS r, vieras.eng_hotels h, vieras.eng_profile_hotel_credentials cr, vieras.vieras_datasources dt
                   where r.FK_HOTEL_ID IN (  ${sqlEngAccount}  )
                      and r.vieras_country = '${countryId}'
                      and r.created between   to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                      and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                      and r.FK_HOTEL_ID = h.ID
                      and h.id = cr.fk_hotel_id
                      and cr.fk_datasource_id = dt.id
                      order by r.created
        """

    sql
  }

}