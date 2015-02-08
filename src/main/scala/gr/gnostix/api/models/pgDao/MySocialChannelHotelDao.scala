package gr.gnostix.api.models.pgDao

import gr.gnostix.api.db.plainsql.DatabaseAccessSupportPg
import gr.gnostix.api.models.plainModels.{ApiData, DataLineGraph, HotelRatingStats, HotelReviewStats, MsgNum, Payload, RevStat, SocialData, SocialDataSum}
import gr.gnostix.api.utilities.DateUtils
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.slick.jdbc.{GetResult, StaticQuery => Q}

/**
 * Created by rebel on 21/10/14.
 */
object MySocialChannelHotelDao extends DatabaseAccessSupportPg {
  implicit val getLineResult = GetResult(r => DataLineGraph(r.<<, r.<<))
  implicit val getTotalResult = GetResult(r => MsgNum(r.<<))
  implicit val getReviewStats = GetResult(r => HotelReviewStats(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))
  implicit val getRatingStats = GetResult(r => HotelRatingStats(r.<<, r.<<))

  val logger = LoggerFactory.getLogger(getClass)


  def getDataCounts(fromDate: DateTime, toDate: DateTime, profileId: Int, dataType: String, datasourceId: Option[Int]): Option[Payload] = {
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

  def getReviewStats(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int,
                     datasourceId: Option[Int]): Future[Option[List[ApiData]]] = {
    val mySqlDynamic = buildQueryStats(fromDate, toDate, profileId, datasourceId)
    //bring the actual data
    val prom = Promise[Option[List[ApiData]]]()

    Future {
      prom.success(getDataStats(mySqlDynamic))
    }
    prom.future
  }

  def getReviewRatingStats(implicit ctx: ExecutionContext, fromDate: DateTime, toDate: DateTime, profileId: Int,
                           datasourceId: Option[Int]): Future[Option[List[ApiData]]] = {
    val mySqlDynamic = buildQueryRatingStats(fromDate, toDate, profileId, datasourceId)
    //bring the actual data
    val prom = Promise[Option[List[ApiData]]]()

    Future {
      prom.success(getDataRatingStats(mySqlDynamic))
    }
    prom.future
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

  private def getDataRatingStats(sql: String): Option[List[ApiData]] = {
    try {
      var myData = List[HotelRatingStats]()
      getConnection withSession {
        implicit session =>
          logger.info("get my hotel rating stats ------------->" + sql)
          val records = Q.queryNA[HotelRatingStats](sql)
          myData = records.list()
      }

      if (myData.size > 0) {
        logger.info(" -------------> we have hotel rating stats ")

        val (neg, pos) = getTopMinusMaxReviews(myData)

        val negative_tips = neg.map(x => s""" ${x.numMsg} reviews mentioned negative your hotel ${x.service_name}""")
        val positive_tips = pos.map(x => s"""Based on ${x.numMsg} reviews, yout hotel ${x.service_name} is mentioned positively""")

        val tips = Map("positive_tips" -> positive_tips, "negative_tips" -> negative_tips)
        // geographic data
        val value = myData.filter(x => x.ratingName.equalsIgnoreCase("value")).groupBy(_.ratingValue)
          .map {
          case (a, s) => (a -> s.size)
        }.toList


        val staff = myData.groupBy(_.ratingName).filter(_._1 contains "staff")
        val room = myData.groupBy(_.ratingName).filter(_._1 contains "room")
        val cleanliness = myData.groupBy(_.ratingName).filter(_._1 contains "cleanliness")
        val sleep = myData.groupBy(_.ratingName).filter(_._1 contains "sleep")
        val location = myData.groupBy(_.ratingName).filter(_._1 contains "location")

        val ratingTips = Map("value" -> value, "staff" -> staff, "room" -> room, "cleanliness" -> cleanliness, "sleep" -> sleep, "location" -> location)

        val servicesStats = getServicesAverageRating(myData)

        Some(List(ApiData("servicesStats", servicesStats), ApiData("tips", tips), ApiData("rating_tips_data_test", ratingTips)))

      } else {
        logger.info(" -------------> nodata ")
        Some(List(ApiData("nodata", None)))
      }

    } catch {
      case e: Exception => {
        e.printStackTrace()
        None
      }
    }
  }

  /**
   * returns for the services in this map the average score
   * @param li
   * @return
   */
  private def getServicesAverageRating(li: List[HotelRatingStats]): Map[String, Int] = {

    li.groupBy(_.ratingName).map {
      case (x, y) => (x, y.map(_.ratingValue).sum / y.size)
    }
  }


  private def getTopMinusMaxReviews(li: List[HotelRatingStats]): (List[RevStat], List[RevStat]) = {
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


    val firstStep = li.
      groupBy(_.ratingName).map {
      case (x, y) => (x, y.groupBy(_.ratingValue).map {
        case (a, s) => RevStat(s.head.ratingName, a, s.size)
      })
    }

    val secondStep = firstStep.toStream.map {
      case (q, w) => {
        List(w.toList.sortBy(r => (r.score, r.numMsg)).head,
          w.toList.sortBy(r => (r.score, r.numMsg)).reverse.head)
      }
    }

    val massagedData =
      secondStep.toList.flatten.sortBy(n => (n.score, n.numMsg))

    val neg = massagedData.take(5).toList
    val pos = massagedData.reverse.take(5).toList


    (neg, pos)
  }


  private def getDataStats(sql: String): Option[List[ApiData]] = {

    try {
      var myData = List[HotelReviewStats]()
      getConnection withSession {
        implicit session =>
          logger.info("get my hotel stats ------------->" + sql)
          val records = Q.queryNA[HotelReviewStats](sql)
          myData = records.list()
      }

      if (myData.size > 0) {
        logger.info(" -------------> we have hotel stats ")

        // top boxes stats
        val stats = Map("score" -> myData.head.datasourceHotelRating,
          "outOf" -> myData.head.maxHotelScore,
          "reviewsNum" -> myData.size,
          "positive" -> myData.filter(x => x.vierasReviewRating >= 8).size,
          "negative" -> myData.filter(x => x.vierasReviewRating <= 4).size)

        // stay type graph
        /*        val stayType = myData.groupBy(_.stayType).map{
                  case (name, tuple) => (name -> tuple.size)
                }.toMap*/


        val stayType = {
            val cleanData = myData.filter(x => !x.stayType.isEmpty || x.stayType != null)

            Map("couple" -> cleanData.filter(x => x.stayType.toLowerCase.contains("couple")
              || x.stayType.toLowerCase.contains("partner")).size, //add also partner
              "friend" -> cleanData.filter(_.stayType.toLowerCase.contains("friend")).size,
              "business" -> cleanData.filter(_.stayType.toLowerCase.contains("business")).size,
              "family" -> cleanData.filter(_.stayType.toLowerCase.contains("famil")).size,
              "solo" -> cleanData.filter(x => x.stayType.toLowerCase.contains("solo")
                || x.stayType.toLowerCase.contains("person")).size)
        }

        // geographic data
        val countries = myData.groupBy(_.country).map {
          case (x, y) => (x -> y.size)
        }.toList.sortBy(_._2).toMap

        Some(List(ApiData("stats", stats), ApiData("countries", countries), ApiData("stayType", stayType)))

      } else {
        logger.info(" -------------> nodata ")
        Some(List(ApiData("nodata", None)))
      }

    } catch {
      case e: Exception => {
        e.printStackTrace()
        None
      }
    }
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

  private def buildQueryStats(fromDate: DateTime, toDate: DateTime, profileId: Int, datasourceId: Option[Int]): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)


    val sql = datasourceId match {
      case Some(x) =>
        s"""
        select r.ID,r.REVIEWER ,r.STAY_TYPE, r.VIERAS_COUNTRY, r.VIERAS_TOTAL_RATING as vieras_review_rating,
             h.TOTAL_RATING  as datasource_hotel_rating, vd.ds_rating_scale as max_hotel_rating
        from vieras.ENG_REVIEWS r, vieras.eng_hotels h, vieras.vieras_datasources vd
           where r.FK_HOTEL_ID IN (  SELECT FK_HOTEL_ID FROM vieras.ENG_PROFILE_HOTEL_CREDENTIALS WHERE FK_PROFILE_ID = ${profileId} and FK_DATASOURCE_ID=${x} )
              and r.created between   to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
              and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
              and r.FK_HOTEL_ID = h.ID        and vd.id=${x}
        """
      case None =>
        // in the case that we are getting the total score for all the datasources then we added the 10 manually to our sql query
        s"""
        select r.ID,r.REVIEWER ,r.STAY_TYPE, r.VIERAS_COUNTRY, r.VIERAS_TOTAL_RATING as vieras_review_rating,
             h.TOTAL_RATING  as datasource_hotel_rating, vd.ds_rating_scale as max_hotel_rating
        from vieras.ENG_REVIEWS r, vieras.eng_hotels h, vieras.vieras_datasources vd
           where r.FK_HOTEL_ID IN (  SELECT FK_HOTEL_ID FROM vieras.ENG_PROFILE_HOTEL_CREDENTIALS WHERE FK_PROFILE_ID = ${profileId})
              and r.created between   to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
              and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
              and r.FK_HOTEL_ID = h.ID
         """
    }


    //logger.info("------------->" + sql + "-----------")

    sql
  }

  private def buildQueryRatingStats(fromDate: DateTime, toDate: DateTime, profileId: Int, datasourceId: Option[Int]): String = {

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern(datePattern)
    val fromDateStr: String = fmt.print(fromDate)
    val toDateStr: String = fmt.print(toDate)


    val sql = datasourceId match {
      case Some(x) =>
        s"""
            select hr.VIERAS_RATING_NAME, hr.VIERAS_RATING_VALUE  from vieras.ENG_REVIEWS r, vieras.eng_review_rating hr
                 where FK_HOTEL_ID IN (SELECT FK_HOTEL_ID FROM vieras.ENG_PROFILE_HOTEL_CREDENTIALS WHERE FK_PROFILE_ID = ${profileId} and FK_DATASOURCE_ID=${x} )
                    and r.created between   to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    and r.ID = hr.FK_PID
                    and hr.VIERAS_RATING_NAME is not null
        """
      case None =>
        // in the case that we are getting the total score for all the datasources then we added the 10 manually to our sql query
        s"""
            select hr.VIERAS_RATING_NAME, hr.VIERAS_RATING_VALUE  from vieras.ENG_REVIEWS r, vieras.eng_review_rating hr
                 where FK_HOTEL_ID IN (SELECT FK_HOTEL_ID FROM vieras.ENG_PROFILE_HOTEL_CREDENTIALS WHERE FK_PROFILE_ID = ${profileId}  )
                    and r.created between   to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
                    and r.ID = hr.FK_PID
                    and hr.VIERAS_RATING_NAME is not null
         """
    }


    //logger.info("------------->" + sql + "-----------")

    sql
  }

  /**
   *
   * @param fromDate
   * @param toDate
   * @param profileId
   * @param datasourceId
   * @return a sql query combination of datasource id and credentials id if present on of those or both (pre profile id)
   */
  private def buildQuery(fromDate: DateTime, toDate: DateTime, profileId: Int, dataType: String, datasourceId: Option[Int]): String = {

    val numDays = DateUtils.findNumberOfDays(fromDate, toDate)
    logger.info("------------->" + numDays + "-----------")

    val datePattern = "dd-MM-yyyy HH:mm:ss"
    val sqlEngAccount = datasourceId match {
      case Some(x) => s""" SELECT FK_HOTEL_ID FROM vieras.ENG_PROFILE_HOTEL_CREDENTIALS WHERE FK_PROFILE_ID = ${profileId} and FK_DATASOURCE_ID=${x}  """
      case None => s""" SELECT FK_HOTEL_ID FROM vieras.ENG_PROFILE_HOTEL_CREDENTIALS WHERE FK_PROFILE_ID = ${profileId}  """

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
    select count(*) from vieras.ENG_REVIEWS
      where FK_HOTEL_ID IN ( $sqlEngAccount )
        and created between   to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
	      and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
        """
    sql
  }

  private def getSqlHotelDataLine(numDays: Int, fromDateStr: String, toDateStr: String, profileId: Int, sqlEngAccount: String) = {
    val grouBydate = DateUtils.sqlGrouByDatePg(numDays)

    val sql = s"""
        select count(*),date_trunc('${grouBydate}',created) from vieras.ENG_REVIEWS
          where FK_HOTEL_ID IN ( $sqlEngAccount )
             and created between   to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
             and to_timestamp('${toDateStr}', 'DD-MM-YYYY HH24:MI:SS')
             and date_trunc('${grouBydate}',created) >= to_timestamp('${fromDateStr}', 'DD-MM-YYYY HH24:MI:SS')
        group by date_trunc('${grouBydate}',created)
        order by date_trunc('${grouBydate}',created) asc
                     """
    logger.info("------------>" + sql)
    sql

  }


}



