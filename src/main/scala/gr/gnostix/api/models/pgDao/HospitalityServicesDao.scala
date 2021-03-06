package gr.gnostix.api.models.pgDao


import gr.gnostix.api.db.plainsql.DatabaseAccessSupportPg
import gr.gnostix.api.models.plainModels.{DashboardServicesKPIs, ApiData, RevStat, HotelRatingStats}
import gr.gnostix.api.utilities.SqlUtils
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.slick.jdbc.{GetResult, StaticQuery => Q}

/**
 * Created by rebel on 18/11/14.
 */
case class Sentiment(positive: Int, negative: Int, neutral: Int)

case class HospitalityServicesSentiment(serviceName: String, score: Int, sentiment: Sentiment)

object HospitalityServicesDao extends DatabaseAccessSupportPg {
  val logger = LoggerFactory.getLogger(getClass)
  implicit val getServicesStatsResult = GetResult(r => HotelRatingStats(r.<<, r.<<))


  def getReviewRatingStats(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, userId :Int, profileId: Int,  companyId: Int): Future[Option[ApiData]] = {
    val sqlEngAccount = SqlUtils.buildHotelCredentialsQuery(userId, profileId, companyId)

    val mySqlDynamic = buildQueryRatingSentiment(fromDate, toDate, sqlEngAccount)
    //bring the actual data
    val prom = Promise[Option[ApiData]]()

    Future {
      prom.success(getDataRatingSentiment(mySqlDynamic))
    }
    prom.future
  }

  def getDataServiceByName(implicit ctx: ExecutionContext, serviceName: String, userId :Int, profileId: Int,  companyId: Int,
                           fromDate: DateTime, toDate: DateTime): Future[Option[ApiData]] = {
    implicit val getHospitalitySentimentResult = GetResult(r => Sentiment(r.<<, r.<<, r.<<))

    val sqlEngAccount = SqlUtils.buildHotelCredentialsQuery(userId, profileId, companyId)

    val mySqlDynamic = buildQueryRatingSentimentByName(fromDate, toDate, serviceName: String, sqlEngAccount)
    //bring the actual data
    val prom = Promise[Option[ApiData]]()

    Future {
      prom.success(getDataRatingSentiment(mySqlDynamic))
    }
    prom.future
  }


  private def getDataRatingSentiment(sql: String): Option[ApiData] = {
    /*      ------------ Test data --------------
    * val li = List(HotelRatingStats("Value", 10), HotelRatingStats("Value", 8),
  HotelRatingStats("Value", 10), HotelRatingStats("Value", 8), HotelRatingStats("Value", 6),
  HotelRatingStats("sleep", 6), HotelRatingStats("staff", 6), HotelRatingStats("room", 8),
  HotelRatingStats("location", 10), HotelRatingStats("room", 4), HotelRatingStats("staff", 6),
  HotelRatingStats("room", 4), HotelRatingStats("sleep", 8), HotelRatingStats("Value", 10),
  HotelRatingStats("location", 8), HotelRatingStats("staff", 10), HotelRatingStats("staff", 6),
  HotelRatingStats("clean", 1), HotelRatingStats("staff", 10), HotelRatingStats("location", 6),
  HotelRatingStats("staff", 4), HotelRatingStats("sleep", 6), HotelRatingStats("staff", 8),
  HotelRatingStats("sleep", 6), HotelRatingStats("location", 8), HotelRatingStats("Value", 8),
  HotelRatingStats("clean", 4), HotelRatingStats("clean", 8), HotelRatingStats("staff", 6),
  HotelRatingStats("sleep", 8), HotelRatingStats("clean", 1), HotelRatingStats("location", 10),
  HotelRatingStats("room", 10), HotelRatingStats("sleep", 4), HotelRatingStats("staff", 6),
  HotelRatingStats("location", 10), HotelRatingStats("staff", 8), HotelRatingStats("sleep", 8))*/

    try {
      var myData = List[HotelRatingStats]()
      getConnection withSession {
        implicit session =>
          logger.info("get my hotel rating stats ------------->" + sql)
          val records = Q.queryNA[HotelRatingStats](sql)
          myData = records.list()
      }

      if (myData.size > 0) {

        val services = myData.groupBy(_.ratingName).map {
          case (x, y) =>
            val dashboardServicesKPIs = (y.map(x => x.ratingValue).sum != 0 && y.size != 0) match {
              case true => DashboardServicesKPIs(x, y.filter(a => a.ratingValue >= 7).size, y.filter(a => a.ratingValue <= 4).size,
                y.filter(a => a.ratingValue > 4 && a.ratingValue < 7).size, y.size,
                BigDecimal(y.map(x => x.ratingValue).sum / y.size).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
              )
              case false => DashboardServicesKPIs(x, y.filter(a => a.ratingValue >= 7).size, y.filter(a => a.ratingValue <= 4).size,
                y.filter(a => a.ratingValue > 4 && a.ratingValue < 7).size, y.size, 0 )
            }

            dashboardServicesKPIs
        }.toList.sortBy(_.size).reverse.take(6)

        services.size match {
          case 6 => Some(ApiData("Services", services))
          case x if x < 6 => Some(ApiData("Services", createExtraServices(services)))
        }
      } else {
        Some(ApiData("Services", createServicesEmptyList()))
      }

    }

  }

  // assuming we deal with hotels only , that's why we have hard coded services
  private def createServicesEmptyList(): List[DashboardServicesKPIs] = {
    val s = List("Location", "Room", "Sleep", "Cleanliness", "Value", "Staff")
    s.map(x => DashboardServicesKPIs(x, 0, 0, 0, 0, 0))
  }

  private def createExtraServices(l: List[DashboardServicesKPIs]): List[DashboardServicesKPIs] = {
    logger.info("createExtraServices " + l.size)
    val s = List("Location", "Room", "Sleep", "Cleanliness", "Value", "Staff")
    val k = l.map(x => x.service)
    val emptyServices = s.diff(k).take(6 - k.size).map(x => DashboardServicesKPIs(x, 0, 0, 0, 0, 0))

    l ::: emptyServices
  }


  private def buildQueryRatingSentiment(fromDate: DateTime, toDate: DateTime, sqlEngAccount: String): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)


    val sql =
      s"""
            select hr.VIERAS_RATING_NAME, hr.VIERAS_RATING_VALUE  from vieras.ENG_REVIEWS r, vieras.eng_review_rating hr
                 where FK_HOTEL_ID IN ( ${sqlEngAccount} )
                    and r.created between   to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    and r.ID = hr.fk_review_id
                    and hr.VIERAS_RATING_NAME is not null
        """


    //logger.info("------------->" + sql + "-----------")

    sql
  }


  private def buildQueryRatingSentimentByName(fromDate: DateTime, toDate: DateTime, serviceName: String, sqlEngAccount: String): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)


    val sql =
      s"""
            select hr.VIERAS_RATING_NAME, hr.VIERAS_RATING_VALUE  from vieras.ENG_REVIEWS r, vieras.eng_review_rating hr
                 where FK_HOTEL_ID IN  ( ${sqlEngAccount} )
                    and r.created between   to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    and r.ID = hr.fk_review_id
                    and hr.VIERAS_RATING_NAME = '${serviceName}'
        """


    //logger.info("------------->" + sql + "-----------")

    sql
  }

}